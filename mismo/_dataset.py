from __future__ import annotations

from collections.abc import Iterator
import dataclasses
from textwrap import dedent
from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from ibis.expr.types import Table

from mismo._util import format_table

if TYPE_CHECKING:
    from mismo.block import PBlocking

    TPBlocking = TypeVar("TPBlocking", bound=PBlocking)


@runtime_checkable
class PDataset(Protocol):
    """Thin wrapper around an Ibis Table."""

    @property
    def table(self) -> Table:
        """The underlying Ibis Table."""
        ...

    @property
    def record_id_column(self) -> str:
        """The name of the column that holds the record ID."""
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
    record_id_column: str = "record_id"
    true_label_column: str | None = None

    def __repr__(self) -> str:
        template = dedent(
            f"""\
            {self.__class__.__name__}(
                record_id_column={self.record_id_column},
                true_label_column={self.true_label_column},
                {{table}}
            )
            """
        )
        return format_table(template, "table", self.table)

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

    @property
    def n_possible_pairs(self) -> int:
        """The number of possible pairs."""
        return len(self.left) * len(self.right)

    def scrub_redundant_comparisons(self, blocking: TPBlocking) -> TPBlocking:
        """Remove redundant comparisons from the Blocking.

        For some DatasetPairs this is a no-op. But for some DatasetPairs, like
        DedupeDatasetPair,

          - we don't want to compare A to A (because that is obviously a match)
          - we only want to compare one of A to B and B to A (because that is redundant)
        """
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
    def record_id_column(self) -> str:
        return self.dataset.record_id_column

    @property
    def true_label_column(self) -> str | None:
        return self.dataset.true_label_column

    def scrub_redundant_comparisons(self, blocking: PBlocking) -> PBlocking:
        ids = blocking.blocked_ids
        left_col, right_col = ids.columns
        filtered = ids[ids[left_col] < ids[right_col]]  # type: ignore
        return blocking.replace_blocked_ids(filtered)

    def __repr__(self) -> str:
        template = dedent(
            f"""\
            {self.__class__.__name__}(
                {{table}}
            )
            """
        )
        return format_table(template, "table", self.dataset.table)


@dataclasses.dataclass(frozen=True)
class LinkageDatasetPair(_PairBase):
    """A pair of Datasets that we want to link using Record Linkage."""

    left: PDataset
    right: PDataset

    def scrub_redundant_comparisons(self, blocking: PBlocking) -> PBlocking:
        # No-op for linkages, we want to keep all comparisons
        return blocking
