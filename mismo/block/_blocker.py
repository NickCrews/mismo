from __future__ import annotations

import dataclasses
from functools import cache
from textwrap import dedent
from typing import Protocol, runtime_checkable

from ibis.expr.types import Table

from mismo._dataset import PDatasetPair
from mismo._util import format_table


@runtime_checkable
class PBlocking(Protocol):
    """Record pairs that have been blocked together."""

    @property
    def dataset_pair(self) -> PDatasetPair:
        """The DatasetPair that was blocked."""
        ...

    @property
    def blocked_ids(self) -> Table:
        """A table of (left_id, right_id) pairs"""
        ...

    @property
    def blocked_data(self) -> Table:
        """The dataset pair joined together on the blocked_ids"""
        ...


@dataclasses.dataclass(frozen=True)
class Blocking:
    dataset_pair: PDatasetPair
    blocked_ids: Table

    @property
    def blocked_data(self) -> Table:
        return join_datasets(self.dataset_pair, self.blocked_ids)

    @cache
    def __repr__(self) -> str:
        template = dedent(
            f"""
            {self.__class__.__name__}(
                {{table}}
            )"""
        )
        return format_table(template, "table", self.blocked_data)


@runtime_checkable
class PBlocker(Protocol):
    """A ``PBlocker`` determines which pairs of records should be compared.

    Either you can compare a set of records to itself, or you can compare two
    different sets of records.

    Args:
        dataset_pair: The DatasetPair to block

    Returns:
        A ``Blocking`` object containing the results of the blocking.
    """

    def block(self, dataset_pair: PDatasetPair) -> PBlocking:
        ...


def join_datasets(dataset_pair: PDatasetPair, on: Table) -> Table:
    """Join two datasets together, so that we can compare them."""
    check_id_pairs(on)
    left, right = dataset_pair
    left_t, right_t = left.table, right.table
    left2 = left_t.relabel({col: col + "_l" for col in left_t.columns})
    right2 = right_t.relabel({col: col + "_r" for col in right_t.columns})
    return on.inner_join(  # type: ignore
        left2,
        left.unique_id_column + "_l",
        suffixes=("", "_l"),
    ).inner_join(
        right2,
        right.unique_id_column + "_r",
        suffixes=("", "_r"),
    )


def check_id_pairs(id_pairs: Table) -> None:
    """Check that the id pairs are valid."""
    if len(id_pairs.columns) != 2:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
