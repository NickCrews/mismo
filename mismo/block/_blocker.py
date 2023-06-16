from __future__ import annotations

import dataclasses
from functools import cache
from textwrap import dedent
from typing import Protocol, runtime_checkable

import ibis
from ibis.expr.types import Table

from mismo._dataset import PDatasetPair
from mismo._typing import Self
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

    def __len__(self) -> int:
        """The number of blocked pairs."""
        ...

    def replace_blocked_ids(self, new_id_pairs: Table) -> Self:
        """Return a new Blocking with the blocked_ids replaced by new_id_pairs."""
        ...


@dataclasses.dataclass()
class Blocking:
    dataset_pair: PDatasetPair
    blocked_ids: Table

    @property
    def blocked_data(self) -> Table:
        return join_datasets(self.dataset_pair, self.blocked_ids)

    def replace_blocked_ids(self, new_id_pairs: Table) -> Self:
        return dataclasses.replace(self, blocked_ids=new_id_pairs)

    @cache
    def __repr__(self) -> str:
        template = dedent(
            f"""
            {self.__class__.__name__}(
                {{table}}
            )"""
        )
        return format_table(template, "table", self.blocked_data)

    def __len__(self) -> int:
        try:
            return self._len
        except AttributeError:
            self._len: int = self.blocked_ids.count().execute()
            return self._len

    def __hash__(self) -> int:
        return hash((self.dataset_pair, self.blocked_ids))


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
        """Block a dataset pair into a set of record pairs to compare.

        Implementors are responsible for calling ``scrub_redundant_comparisons`` on
        the ``dataset_pair`` before returning the ``Blocking`` object.
        TODO: Should it be this object's responsibility?
        """
        ...


class CartesianBlocker(PBlocker):
    """Block all possible pairs of records (i.e. the Cartesian product).)"""

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
        left, right = dataset_pair
        lid = left.record_id_column
        rid = right.record_id_column
        lid_new = lid + "_l"
        rid_new = rid + "_r"
        left_ids = left.table.select(lid).relabel({lid: lid_new})
        right_ids = right.table.select(rid).relabel({rid: rid_new})
        blocked_ids = left_ids.cross_join(right_ids)
        b = Blocking(dataset_pair, blocked_ids)
        return dataset_pair.scrub_redundant_comparisons(b)


class FunctionBlocker(PBlocker):
    """Blocks based on a function of type (Table, Table) -> Boolean"""

    def __init__(self, func):
        self.func = func

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
        left, right = dataset_pair
        lid = left.record_id_column
        rid = right.record_id_column
        lt = left.table.relabel({lid: lid + "_l"})
        rt = right.table.relabel({rid: rid + "_r"})
        joined_full = ibis.join(lt, rt, predicates=self.func(lt, rt), how="inner")
        ids = joined_full[lid + "_l", rid + "_r"]
        b = Blocking(dataset_pair, ids)
        return dataset_pair.scrub_redundant_comparisons(b)


def join_datasets(dataset_pair: PDatasetPair, on: Table) -> Table:
    """Join two datasets together, so that we can compare them."""
    check_id_pairs(on)
    left, right = dataset_pair
    left_t, right_t = left.table, right.table
    left2 = left_t.relabel({col: col + "_l" for col in left_t.columns})
    right2 = right_t.relabel({col: col + "_r" for col in right_t.columns})
    return on.inner_join(  # type: ignore
        left2,
        left.record_id_column + "_l",
        suffixes=("", "_l"),
    ).inner_join(
        right2,
        right.record_id_column + "_r",
        suffixes=("", "_r"),
    )


def check_id_pairs(id_pairs: Table) -> None:
    """Check that the id pairs are valid."""
    if len(id_pairs.columns) != 2:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
