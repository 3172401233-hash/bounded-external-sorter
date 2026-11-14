import json
import os
import random
import tempfile
import threading
import unittest
from unittest import mock

from bounded_external_sorter import (
    MIN_MEMORY_BUDGET_BYTES,
    RECORD_OVERHEAD_BYTES,
    ExternalSorter,
    InsufficientMemoryError,
    MergeInputError,
    SortCancelledError,
    TempFileWriteError,
)


def line(**fields):
    return json.dumps(fields).encode("utf-8")


def payload_ids(records):
    return [json.loads(r)["id"] for r in records]


class MultiKeySortTest(unittest.TestCase):
    def test_multi_key_ordering(self):
        rows = [
            line(a=2, b="x", id=0),
            line(a=1, b="y", id=1),
            line(a=2, b="w", id=2),
            line(a=1, b="y", id=3),
            line(a=1, b="a", id=4),
        ]
        result = ExternalSorter(1 << 20, keys=["a", "b"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [4, 1, 3, 2, 0])
        self.assertEqual(result.rejected, [])
        self.assertEqual(result.runs, [])

    def test_numeric_key_mixes_int_and_float(self):
        rows = [line(v=2, id=0), line(v=1.5, id=1), line(v=-1, id=2), line(v=1.5, id=3)]
        result = ExternalSorter(1 << 20, keys=["v"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [2, 1, 3, 0])

    def test_string_key_ordering(self):
        rows = [line(v="pear", id=0), line(v="apple", id=1), line(v="banana", id=2)]
        result = ExternalSorter(1 << 20, keys=["v"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [1, 2, 0])


class NullOrderingTest(unittest.TestCase):
    ROWS = [
        line(v=None, id=0),
        line(v=3, id=1),
        line(other=1, id=2),  # missing key counts as null
        line(v=1, id=3),
    ]

    def test_nulls_last_by_default(self):
        result = ExternalSorter(1 << 20, keys=["v"]).sort(self.ROWS)
        self.assertEqual(payload_ids(result.records), [3, 1, 0, 2])

    def test_nulls_first(self):
        result = ExternalSorter(1 << 20, keys=["v"], nulls="first").sort(self.ROWS)
        self.assertEqual(payload_ids(result.records), [0, 2, 3, 1])

    def test_invalid_nulls_config_rejected(self):
        with self.assertRaises(ValueError):
            ExternalSorter(1 << 20, keys=["v"], nulls="middle")


class RejectedLinesTest(unittest.TestCase):
    def test_bad_lines_keep_raw_bytes(self):
        bad = [b"{not json", b"[1, 2]", b'"just a string"', b'{"v": NaN}']
        good = [line(v=2, id=10), line(v=1, id=11)]
        rows = [good[0], bad[0], bad[1], good[1], bad[2], bad[3]]
        result = ExternalSorter(1 << 20, keys=["v"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [11, 10])
        self.assertEqual([r.raw for r in result.rejected], bad)
        self.assertEqual([r.seq for r in result.rejected], [1, 2, 4, 5])
        self.assertTrue(all(r.reason for r in result.rejected))

    def test_mixed_number_and_string_key_rejected(self):
        rows = [line(v=2, id=0), line(v="abc", id=1), line(v=1, id=2)]
        result = ExternalSorter(1 << 20, keys=["v"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [2, 0])
        self.assertEqual(len(result.rejected), 1)
        self.assertEqual(result.rejected[0].raw, rows[1])
        self.assertIn("mixes", result.rejected[0].reason)

    def test_unsupported_key_types_rejected(self):
        rows = [
            line(v=1, id=0),
            line(v=True, id=1),
            line(v=[1], id=2),
            line(v={"x": 1}, id=3),
        ]
        result = ExternalSorter(1 << 20, keys=["v"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [0])
        self.assertEqual([r.seq for r in result.rejected], [1, 2, 3])


class StabilityTest(unittest.TestCase):
    def test_duplicate_keys_keep_input_order(self):
        rows = [line(k=5, id=i) for i in range(50)]
        tiny = MIN_MEMORY_BUDGET_BYTES
        result = ExternalSorter(tiny, keys=["k"]).sort(rows)
        self.assertEqual(payload_ids(result.records), list(range(50)))
        self.assertGreater(len(result.runs), 1)

    def test_tie_break_spans_multiple_keys(self):
        rows = [
            line(a=1, b=1, id=0),
            line(a=1, b=1, id=1),
            line(a=1, b=0, id=2),
            line(a=1, b=1, id=3),
        ]
        result = ExternalSorter(1 << 20, keys=["a", "b"]).sort(rows)
        self.assertEqual(payload_ids(result.records), [2, 0, 1, 3])


class SpillAndBudgetTest(unittest.TestCase):
    def _dataset(self, n=120, seed=7):
        rng = random.Random(seed)
        rows = []
        for i in range(n):
            rows.append(
                line(
                    a=rng.choice([1, 2, 3, None]),
                    b=rng.choice(["x", "y", "z"]),
                    id=i,
                )
            )
        return rows

    def test_tiny_budget_forces_many_spills_with_seq_ranges(self):
        rows = self._dataset()
        result = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["a", "b"]).sort(rows)
        self.assertGreater(len(result.runs), 3)
        total = sum(run.count for run in result.runs)
        self.assertEqual(total, len(rows))
        for index, run in enumerate(result.runs):
            self.assertEqual(run.index, index)
            self.assertLessEqual(run.start_seq, run.end_seq)
            self.assertGreaterEqual(run.start_seq, 0)
            self.assertLess(run.end_seq, len(rows))

    def test_order_is_identical_across_budgets(self):
        rows = self._dataset()
        big = ExternalSorter(1 << 20, keys=["a", "b"]).sort(rows)
        tiny = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["a", "b"]).sort(rows)
        mid = ExternalSorter(MIN_MEMORY_BUDGET_BYTES * 4, keys=["a", "b"]).sort(rows)
        self.assertEqual(big.records, tiny.records)
        self.assertEqual(big.records, mid.records)
        self.assertEqual([r.raw for r in big.rejected], [r.raw for r in tiny.rejected])

    def test_budget_below_minimum_rejected(self):
        with self.assertRaises(InsufficientMemoryError):
            ExternalSorter(MIN_MEMORY_BUDGET_BYTES - 1, keys=["a"])

    def test_single_record_larger_than_budget(self):
        big_line = line(v=1, pad="x" * MIN_MEMORY_BUDGET_BYTES)
        self.assertGreater(len(big_line) + RECORD_OVERHEAD_BYTES, MIN_MEMORY_BUDGET_BYTES)
        sorter = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["v"])
        with self.assertRaises(InsufficientMemoryError):
            sorter.sort([big_line])

    def test_empty_input(self):
        result = ExternalSorter(1 << 20, keys=["v"]).sort([])
        self.assertEqual(result.records, [])
        self.assertEqual(result.rejected, [])
        self.assertEqual(result.runs, [])


class TempWriteFailureTest(unittest.TestCase):
    def test_uncreatable_spill_directory(self):
        with tempfile.NamedTemporaryFile() as blocker:
            sorter = ExternalSorter(
                MIN_MEMORY_BUDGET_BYTES, keys=["v"], temp_dir=blocker.name
            )
            with self.assertRaises(TempFileWriteError):
                sorter.sort([line(v=i, id=i) for i in range(20)])

    def test_write_failure_mid_spill_cleans_up(self):
        with tempfile.TemporaryDirectory() as tmp:
            sorter = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["v"], temp_dir=tmp)
            real_open = open

            def failing_open(path, mode="r", *args, **kwargs):
                if "b" in mode and str(path).endswith(".bin"):
                    raise OSError("simulated disk failure")
                return real_open(path, mode, *args, **kwargs)

            with mock.patch("builtins.open", side_effect=failing_open):
                with self.assertRaises(TempFileWriteError):
                    sorter.sort([line(v=i, id=i) for i in range(20)])
            self.assertEqual(os.listdir(tmp), [])


class MergeTest(unittest.TestCase):
    def test_merge_requires_at_least_two_inputs(self):
        sorter = ExternalSorter(1 << 20, keys=["v"])
        with self.assertRaises(MergeInputError):
            sorter.merge_runs([])
        with self.assertRaises(MergeInputError):
            sorter.merge_runs(["only-one.bin"])

    def test_merge_is_deterministic_across_repeats(self):
        rng = random.Random(11)
        rows = [line(v=rng.randint(0, 5), id=i) for i in range(200)]
        first = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["v"]).sort(rows)
        second = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["v"]).sort(rows)
        self.assertEqual(first.records, second.records)
        self.assertEqual(payload_ids(first.records), sorted(
            payload_ids(first.records),
            key=lambda i: (json.loads(rows[i])["v"], i),
        ))


class CancellationTest(unittest.TestCase):
    def test_cancel_before_start(self):
        event = threading.Event()
        event.set()
        sorter = ExternalSorter(1 << 20, keys=["v"], cancel_event=event)
        with self.assertRaises(SortCancelledError):
            sorter.sort([line(v=1)])

    def test_cancel_mid_stream_leaves_no_temp_files(self):
        event = threading.Event()

        def lines():
            for i in range(500):
                if i == 30:
                    event.set()
                yield line(v=i % 7, id=i)

        with tempfile.TemporaryDirectory() as tmp:
            sorter = ExternalSorter(
                MIN_MEMORY_BUDGET_BYTES, keys=["v"], temp_dir=tmp, cancel_event=event
            )
            with self.assertRaises(SortCancelledError):
                sorter.sort(lines())
            self.assertEqual(os.listdir(tmp), [])

    def test_successful_sort_leaves_no_temp_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            sorter = ExternalSorter(MIN_MEMORY_BUDGET_BYTES, keys=["v"], temp_dir=tmp)
            result = sorter.sort([line(v=i % 3, id=i) for i in range(100)])
            self.assertEqual(len(result.records), 100)
            self.assertGreater(len(result.runs), 1)
            self.assertEqual(os.listdir(tmp), [])


if __name__ == "__main__":
    unittest.main()
