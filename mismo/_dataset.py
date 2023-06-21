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
class PDatasetPair(Protocol):
    """A pair of Tables that we want to link."""

    @property
    def left(self) -> Table:
        """The left table."""
        ...

    @property
    def right(self) -> Table:
        """The right table."""
        ...

    def __iter__(self) -> Iterator[Table]:
        """Iterate over the left and right tables."""
        ...

    @property
    def n_possible_pairs(self) -> int:
        """The number of possible pairs."""
        ...

    def scrub_redundant_comparisons(self, blocking: TPBlocking) -> TPBlocking:
        """Remove redundant comparisons from the Blocking.

        For some DatasetPairs this is a no-op. But for some DatasetPairs, like
        DedupeDatasetPair,

          - we don't want to compare A to A (because that is obviously a match)
          - we only want to compare one of A to B and B to A (because that is redundant)
        """
        ...


class _PairBase(PDatasetPair, Protocol):
    def __iter__(self) -> Iterator[Table]:
        return iter((self.left, self.right))


@dataclasses.dataclass(frozen=True)
class DedupeDatasetPair(_PairBase):
    """A single Table to dedupe, with the interface of a DatasetPair."""

    table: Table

    def __post_init__(self) -> None:
        check_dataset(self.table)

    @property
    def left(self) -> Table:
        return self.table

    @property
    def right(self) -> Table:
        return self.table

    def scrub_redundant_comparisons(self, blocking: TPBlocking) -> TPBlocking:
        ids = blocking.blocked_ids
        left_col, right_col = ids.columns
        filtered = ids[ids[left_col] < ids[right_col]]
        return blocking.replace_blocked_ids(filtered)

    @property
    def n_possible_pairs(self) -> int:
        n = self.table.count().execute()
        return n * (n - 1) // 2

    def __repr__(self) -> str:
        template = dedent(
            f"""\
            {self.__class__.__name__}(
                {{table}}
            )
            """
        )
        return format_table(template, "table", self.table)


@dataclasses.dataclass(frozen=True)
class LinkageDatasetPair(_PairBase):
    """Two different Tables to link together."""

    left: Table
    right: Table

    def __post_init__(self) -> None:
        check_dataset(self.left)
        check_dataset(self.right)

    def scrub_redundant_comparisons(self, blocking: TPBlocking) -> TPBlocking:
        # No-op for linkages, we want to keep all comparisons
        return blocking

    @property
    def n_possible_pairs(self) -> int:
        return (self.left.count() * self.right.count()).execute()


def check_dataset(dataset: Table) -> None:
    """Ensure that a table follows the mismo dataset conventions.

    A dataset table MUST:
      - have a column called "record_id" that is unique
    """
    if "record_id" not in dataset.columns:
        raise ValueError(
            f"Dataset must have a column called 'record_id', but got {dataset.columns}"
        )
    if (dataset.count() != dataset.distinct(on="record_id").count()).execute():
        raise ValueError("Dataset's 'record_id' column must be unique")
