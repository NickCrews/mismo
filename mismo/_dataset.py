from __future__ import annotations

from textwrap import dedent
from typing import Protocol, runtime_checkable

from ibis.expr.types import Table


@runtime_checkable
class PDataset(Protocol):
    """Thin wrapper around an Ibis Table."""

    @property
    def table(self) -> Table:
        """The underlying Ibis Table."""
        ...

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


class Dataset(PDataset):
    def __init__(
        self, table: Table, unique_id_column: str, true_label_column: str | None = None
    ):
        self._table = table
        self._unique_id_column = unique_id_column
        self._true_label_column = true_label_column

    @property
    def table(self) -> Table:
        return self._table

    @property
    def unique_id_column(self) -> str:
        return self._unique_id_column

    @property
    def true_label_column(self) -> str | None:
        return self._true_label_column

    def __repr__(self) -> str:
        return dedent(
            f"""{self.__class__.__name__}(
                    unique_id_column={self.unique_id_column},
                    true_label_column={self.true_label_column},
                    {self.table.head(5)!r}
                )"""
        )


@runtime_checkable
class PDatasetPair(Protocol):
    """A pair of Datasets that we want to link."""

    @property
    def left(self) -> PDataset:
        """The left dataset."""
        ...

    @property
    def right(self) -> PDataset:
        """The right dataset."""
        ...

    def __iter__(self):
        return iter((self.left, self.right))


class DedupeDatasetPair(PDatasetPair):
    """A pair of Datasets that we want to link using Dedupe."""

    def __init__(self, data: PDataset):
        self._data = data

    @property
    def left(self) -> PDataset:
        return self._data

    @property
    def right(self) -> PDataset:
        return self._data

    @property
    def id_column(self) -> str:
        return self._data.unique_id_column

    @property
    def true_label_column(self) -> str | None:
        return self._data.true_label_column

    def __repr__(self) -> str:
        return dedent(
            f"""
            {self.__class__.__name__}(
                {self.left!r}
            )"""
        )


class LinkageDatasetPair(PDatasetPair):
    """A pair of Datasets that we want to link using Record Linkage."""

    def __init__(self, left: PDataset, right: PDataset):
        self._left = left
        self._right = right

    @property
    def left(self) -> PDataset:
        return self._left

    @property
    def right(self) -> PDataset:
        return self._right
