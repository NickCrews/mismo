from __future__ import annotations

from collections.abc import Iterator
import dataclasses
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
        ...

    @property
    def true_label_column(self) -> str | None:
        """
        The name of the column with the true label, or None if there is no true label.
        """
        ...

    def __len__(self) -> int:
        """The number of records in the dataset."""
        ...


@dataclasses.dataclass(frozen=True)
class Dataset:
    table: Table
    unique_id_column: str = "record_id"
    true_label_column: str | None = None

    def __repr__(self) -> str:
        return dedent(
            f"""{self.__class__.__name__}(
                    unique_id_column={self.unique_id_column},
                    true_label_column={self.true_label_column},
                    {self.table.head(5)!r}
                )"""
        )

    def __len__(self) -> int:
        return self.table.count().execute()  # type: ignore


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

    def __iter__(self) -> Iterator[PDataset]:
        """Iterate over the left and right datasets."""
        ...


class _PairBase(PDatasetPair, Protocol):
    def __iter__(self) -> Iterator[PDataset]:
        return iter((self.left, self.right))


@dataclasses.dataclass(frozen=True)
class DedupeDatasetPair(_PairBase):
    """A pair of Datasets that we want to link using Dedupe."""

    dataset: PDataset

    @property
    def left(self) -> PDataset:
        return self.dataset

    @property
    def right(self) -> PDataset:
        return self.dataset

    @property
    def id_column(self) -> str:
        return self.dataset.unique_id_column

    @property
    def true_label_column(self) -> str | None:
        return self.dataset.true_label_column

    def __repr__(self) -> str:
        return dedent(
            f"""
            {self.__class__.__name__}(
                {self.dataset!r}
            )"""
        )


@dataclasses.dataclass(frozen=True)
class LinkageDatasetPair(_PairBase):
    """A pair of Datasets that we want to link using Record Linkage."""

    left: PDataset
    right: PDataset
