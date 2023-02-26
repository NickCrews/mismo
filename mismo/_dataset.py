from __future__ import annotations

from typing import Protocol, runtime_checkable

from ibis.expr.types import Table


@runtime_checkable
class Dataset(Protocol):
    """Thin wrapper around an Ibis Table."""

    @property
    def table(self) -> Table:
        """The underlying Ibis Table."""

    @property
    def unique_id_column(self) -> str:
        """The name of the column that uniquely identifies each record."""
        return "record_id"

    @property
    def true_label_column(self) -> str | None:
        """
        The name of the column with the true label, or None if there is no true label.
        """
        return None

    def __len__(self) -> int:
        """The number of records in the dataset."""
        return self.table.count().execute()  # type: ignore


@runtime_checkable
class DatasetPair(Protocol):
    """A pair of Datasets that we want to link."""

    @property
    def left(self) -> Dataset:
        """The left dataset."""

    @property
    def right(self) -> Dataset:
        """The right dataset."""

    def __iter__(self):
        return iter((self.left, self.right))


class DedupeDatasetPair(DatasetPair):
    """A pair of Datasets that we want to link using Dedupe."""

    def __init__(self, data: Dataset):
        self._data = data

    @property
    def left(self) -> Dataset:
        return self._data

    @property
    def right(self) -> Dataset:
        return self._data

    @property
    def id_column(self) -> str:
        return self._data.unique_id_column


class LinkageDatasetPair(DatasetPair):
    """A pair of Datasets that we want to link using Record Linkage."""

    def __init__(self, left: Dataset, right: Dataset):
        self._left = left
        self._right = right

    @property
    def left(self) -> Dataset:
        return self._left

    @property
    def right(self) -> Dataset:
        return self._right
