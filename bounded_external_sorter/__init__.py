"""Bounded-memory stable external sorting for JSONL records."""

from .errors import (
    InsufficientMemoryError,
    MergeInputError,
    SortCancelledError,
    SorterError,
    TempFileWriteError,
)
from .sorter import (
    MIN_MEMORY_BUDGET_BYTES,
    RECORD_OVERHEAD_BYTES,
    ExternalSorter,
    RejectedRecord,
    RunInfo,
    SortKey,
    SortResult,
)

__all__ = [
    "ExternalSorter",
    "SortKey",
    "SortResult",
    "RejectedRecord",
    "RunInfo",
    "SorterError",
    "InsufficientMemoryError",
    "TempFileWriteError",
    "MergeInputError",
    "SortCancelledError",
    "RECORD_OVERHEAD_BYTES",
    "MIN_MEMORY_BUDGET_BYTES",
]
