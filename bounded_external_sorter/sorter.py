"""Bounded-memory external sorting for JSONL records.

The sorter never holds more than roughly ``memory_budget`` bytes of record
payload in memory.  Records are buffered until the budget would be exceeded,
sorted in memory (stably, by original sequence number), and spilled to a
temporary run file.  All runs are then combined with a stable k-way merge.
"""

from __future__ import annotations

import heapq
import json
import math
import os
import shutil
import struct
import tempfile
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .errors import (
    InsufficientMemoryError,
    MergeInputError,
    SortCancelledError,
    SorterError,
    TempFileWriteError,
)

# Estimated per-record interpreter overhead (tuple, dict entries, key list)
# added on top of the raw line length when accounting memory usage.
RECORD_OVERHEAD_BYTES = 128

# Budgets below this floor are rejected up front: nothing useful can be
# buffered and the merge bookkeeping itself needs some headroom.
MIN_MEMORY_BUDGET_BYTES = 512

_CANCEL_CHECK_INTERVAL = 1024
_RUN_MAGIC = "bounded-external-sorter/run-v1"

_CATEGORY_NUMBER = "number"
_CATEGORY_STRING = "string"


@dataclass(frozen=True)
class SortKey:
    """A single sort key: the name of a top-level JSON object field."""

    name: str


@dataclass(frozen=True)
class RejectedRecord:
    """A line that could not be sorted, with its original bytes preserved."""

    seq: int
    raw: bytes
    reason: str


@dataclass(frozen=True)
class RunInfo:
    """Metadata for one spill run: sequence range and record count."""

    index: int
    count: int
    start_seq: int
    end_seq: int


@dataclass
class SortResult:
    """Outcome of a sort: ordered raw records, rejected lines, run metadata."""

    records: List[bytes]
    rejected: List[RejectedRecord]
    runs: List[RunInfo]


def _category_of(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "unsupported"
    if isinstance(value, (int, float)):
        return _CATEGORY_NUMBER
    if isinstance(value, str):
        return _CATEGORY_STRING
    return "unsupported"


def _encode_value(value: Any, nulls_first: bool) -> tuple:
    """Encode a key value into a comparable tuple.

    Nulls sort below everything when ``nulls_first`` is true, above
    everything otherwise.  Numbers and strings share a rank slot but are
    never mixed within one key column (enforced by the caller), so the
    category discriminator keeps the encoding total and deterministic.
    """
    if value is None:
        return (0,) if nulls_first else (2,)
    if isinstance(value, str):
        return (1, 1, value)
    return (1, 0, value)


def _normalize_line(line: Any) -> bytes:
    if isinstance(line, bytes):
        return line
    if isinstance(line, (bytearray, memoryview)):
        return bytes(line)
    if isinstance(line, str):
        return line.encode("utf-8")
    raise TypeError(f"lines must be bytes or str, got {type(line).__name__}")


def _parse_record(raw: bytes) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        obj = json.loads(raw)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        return None, f"invalid JSON: {exc}"
    if not isinstance(obj, dict):
        return None, "record is not a JSON object"
    return obj, None


def _read_exact(fh, n: int) -> bytes:
    data = fh.read(n)
    if len(data) != n:
        raise SorterError("truncated or corrupt run file")
    return data


class ExternalSorter:
    """Stable external sort over JSONL records with an explicit memory budget.

    Parameters
    ----------
    memory_budget:
        Hard budget in bytes for the in-memory record buffer.  Estimated
        usage per record is ``len(raw_line) + RECORD_OVERHEAD_BYTES``.
    keys:
        One or more sort keys (field names or :class:`SortKey`), applied
        left to right.  All keys sort ascending.
    nulls:
        ``"first"`` sorts null (and missing) key values before everything
        else; ``"last"`` sorts them after everything else.
    temp_dir:
        Parent directory for spill files.  A private subdirectory is created
        per sort and always removed afterwards, including on cancellation.
    cancel_event:
        Optional ``threading.Event``-like object; when set, the sort aborts
        with :class:`SortCancelledError` and leaves no temp files behind.
    """

    def __init__(
        self,
        memory_budget: int,
        *,
        keys: Sequence[Any],
        nulls: str = "last",
        temp_dir: Optional[str] = None,
        cancel_event: Any = None,
    ) -> None:
        if isinstance(memory_budget, bool) or not isinstance(memory_budget, (int, float)):
            raise InsufficientMemoryError("memory_budget must be a number of bytes")
        if int(memory_budget) < MIN_MEMORY_BUDGET_BYTES:
            raise InsufficientMemoryError(
                f"memory budget {int(memory_budget)} bytes is below the minimum "
                f"of {MIN_MEMORY_BUDGET_BYTES} bytes"
            )
        if nulls not in ("first", "last"):
            raise ValueError("nulls must be 'first' or 'last'")
        normalized = [k if isinstance(k, SortKey) else SortKey(str(k)) for k in keys]
        if not normalized:
            raise ValueError("at least one sort key is required")
        self._budget = int(memory_budget)
        self._keys = normalized
        self._nulls_first = nulls == "first"
        self._temp_dir = temp_dir
        self._cancel_event = cancel_event

    # ------------------------------------------------------------------ API

    def sort(self, lines: Iterable[Any]) -> SortResult:
        """Sort JSONL ``lines`` and return a :class:`SortResult`."""
        workdir = self._make_workdir()
        try:
            return self._sort_in_workdir(lines, workdir)
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    def merge_runs(self, run_paths: Sequence[str]) -> List[bytes]:
        """Merge existing run files into one ordered list of raw records."""
        return list(self._merge(list(run_paths)))

    # -------------------------------------------------------------- intern

    def _check_cancel(self) -> None:
        if self._cancel_event is not None and self._cancel_event.is_set():
            raise SortCancelledError("sort cancelled by caller")

    def _make_workdir(self) -> str:
        try:
            if self._temp_dir is not None:
                os.makedirs(self._temp_dir, exist_ok=True)
            return tempfile.mkdtemp(prefix="besort-", dir=self._temp_dir)
        except OSError as exc:
            raise TempFileWriteError(f"cannot create spill directory: {exc}") from exc

    def _extract_key(self, obj: Dict[str, Any], categories: List[Optional[str]]):
        encoded = []
        for i, key in enumerate(self._keys):
            value = obj.get(key.name)
            category = _category_of(value)
            if category == "unsupported":
                return None, (
                    f"key {key.name!r}: unsupported value type "
                    f"{type(value).__name__}"
                )
            if category is not None:
                if isinstance(value, float) and not math.isfinite(value):
                    return None, f"key {key.name!r}: non-finite number"
                if categories[i] is None:
                    categories[i] = category
                elif categories[i] != category:
                    return None, (
                        f"key {key.name!r}: mixes numeric and string values"
                    )
            encoded.append(_encode_value(value, self._nulls_first))
        return tuple(encoded), None

    def _sort_in_workdir(self, lines: Iterable[Any], workdir: str) -> SortResult:
        categories: List[Optional[str]] = [None] * len(self._keys)
        buffer: List[Tuple[tuple, int, bytes]] = []
        used = 0
        rejected: List[RejectedRecord] = []
        runs: List[RunInfo] = []
        run_paths: List[str] = []
        seq = 0
        for line in lines:
            self._check_cancel()
            raw = _normalize_line(line)
            obj, error = _parse_record(raw)
            key: Optional[tuple] = None
            if error is None:
                key, error = self._extract_key(obj, categories)
            if error is not None:
                rejected.append(RejectedRecord(seq=seq, raw=raw, reason=error))
            else:
                estimated = len(raw) + RECORD_OVERHEAD_BYTES
                if estimated > self._budget:
                    raise InsufficientMemoryError(
                        f"record #{seq} needs ~{estimated} bytes, which exceeds "
                        f"the memory budget of {self._budget} bytes"
                    )
                if buffer and used + estimated > self._budget:
                    self._spill(buffer, runs, run_paths, workdir)
                    buffer = []
                    used = 0
                buffer.append((key, seq, raw))
                used += estimated
            seq += 1

        if runs:
            if buffer:
                self._spill(buffer, runs, run_paths, workdir)
            if len(run_paths) == 1:
                with open(run_paths[0], "rb") as fh:
                    records = [raw for _, _, raw in self._iter_run(fh)]
            else:
                records = list(self._merge(run_paths))
        else:
            buffer.sort(key=lambda item: (item[0], item[1]))
            records = [raw for _, _, raw in buffer]
        return SortResult(records=records, rejected=rejected, runs=runs)

    def _spill(
        self,
        buffer: List[Tuple[tuple, int, bytes]],
        runs: List[RunInfo],
        run_paths: List[str],
        workdir: str,
    ) -> None:
        self._check_cancel()
        buffer.sort(key=lambda item: (item[0], item[1]))
        seqs = [item[1] for item in buffer]
        index = len(runs)
        path = os.path.join(workdir, f"run-{index:05d}.bin")
        header = json.dumps(
            {
                "magic": _RUN_MAGIC,
                "index": index,
                "count": len(buffer),
                "start_seq": min(seqs),
                "end_seq": max(seqs),
            }
        ).encode("utf-8")
        try:
            with open(path, "xb") as fh:
                fh.write(struct.pack(">I", len(header)))
                fh.write(header)
                for key, seq, raw in buffer:
                    meta = json.dumps([seq, list(key)]).encode("utf-8")
                    fh.write(struct.pack(">II", len(meta), len(raw)))
                    fh.write(meta)
                    fh.write(raw)
        except OSError as exc:
            try:
                os.unlink(path)
            except OSError:
                pass
            raise TempFileWriteError(
                f"failed to write spill file {path}: {exc}"
            ) from exc
        runs.append(
            RunInfo(index=index, count=len(buffer), start_seq=min(seqs), end_seq=max(seqs))
        )
        run_paths.append(path)

    def _iter_run(self, fh) -> Iterator[Tuple[list, int, bytes]]:
        (header_len,) = struct.unpack(">I", _read_exact(fh, 4))
        header = json.loads(_read_exact(fh, header_len).decode("utf-8"))
        if header.get("magic") != _RUN_MAGIC:
            raise SorterError("not a valid bounded-external-sorter run file")
        for _ in range(header["count"]):
            meta_len, raw_len = struct.unpack(">II", _read_exact(fh, 8))
            meta = json.loads(_read_exact(fh, meta_len).decode("utf-8"))
            raw = _read_exact(fh, raw_len)
            yield meta[1], meta[0], raw

    def _merge(self, paths: List[str]) -> Iterator[bytes]:
        if len(paths) < 2:
            raise MergeInputError(
                f"k-way merge requires at least 2 input runs, got {len(paths)}"
            )
        self._check_cancel()
        with ExitStack() as stack:
            files = [stack.enter_context(open(p, "rb")) for p in paths]
            heap: List[Tuple[list, int, int, bytes, Any]] = []
            for run_id, fh in enumerate(files):
                iterator = self._iter_run(fh)
                try:
                    key, seq, raw = next(iterator)
                except StopIteration:
                    continue
                heapq.heappush(heap, (key, seq, run_id, raw, iterator))
            pops = 0
            while heap:
                key, seq, run_id, raw, iterator = heapq.heappop(heap)
                yield raw
                pops += 1
                if pops % _CANCEL_CHECK_INTERVAL == 0:
                    self._check_cancel()
                try:
                    next_key, next_seq, next_raw = next(iterator)
                except StopIteration:
                    continue
                heapq.heappush(heap, (next_key, next_seq, run_id, next_raw, iterator))
