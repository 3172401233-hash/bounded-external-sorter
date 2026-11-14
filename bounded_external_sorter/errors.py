"""Exception hierarchy for bounded_external_sorter."""


class SorterError(Exception):
    """Base class for all sorter errors."""


class InsufficientMemoryError(SorterError):
    """The configured memory budget cannot hold even a single record."""


class TempFileWriteError(SorterError):
    """A spill (temporary run) file could not be created or written."""


class MergeInputError(SorterError):
    """The k-way merge was invoked with too few input runs."""


class SortCancelledError(SorterError):
    """The caller cancelled the sort via the configured cancel event."""
