from __future__ import annotations

from functools import cache
from textwrap import dedent
from typing import Protocol, runtime_checkable

from ibis.expr.types import Table

from mismo._dataset import PDatasetPair
from mismo._util import format_table


class Blocking:
    _dataset_pair: PDatasetPair
    _blocked_ids: Table
    _blocked_data: Table

    def __init__(
        self,
        dataset_pair: PDatasetPair,
        *,  # force keyword arguments
        blocked_ids: Table | None = None,
        blocked_data: Table | None = None,
    ) -> None:
        self._dataset_pair = dataset_pair
        if blocked_ids is None and blocked_data is None:
            raise ValueError("Must provide either blocked_ids or blocked_data")
        if blocked_ids is not None and blocked_data is not None:
            raise ValueError("Must provide only one of blocked_ids or blocked_data")
        if blocked_ids is not None:
            REQUIRED_ID_COLUMNS = {"record_id_l", "record_id_r"}
            if set(blocked_ids.columns) != REQUIRED_ID_COLUMNS:
                raise ValueError(
                    f"Expected blocked_ids to have columns {REQUIRED_ID_COLUMNS}, "
                    f"but it has {blocked_ids.columns}"
                )
            self._blocked_ids = blocked_ids
            left, right = self.dataset_pair
            self._blocked_data = _join_tables(left, right, self.blocked_ids)
        else:
            self._blocked_data = blocked_data
            self._blocked_ids = blocked_data["record_id_l", "record_id_r"]

    @property
    def dataset_pair(self) -> PDatasetPair:
        """The DatasetPair that was blocked."""
        return self._dataset_pair

    @property
    def blocked_ids(self) -> Table:
        """A table of (left_id, right_id) pairs"""
        return self._blocked_ids

    @property
    def blocked_data(self) -> Table:
        """The dataset pair joined together on the blocked_ids"""
        return self._blocked_data

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
        """The number of blocked pairs."""
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

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
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
        left_ids = left.select("record_id").relabel("{name}_l")
        right_ids = right.select("record_id").relabel("{name}_r")
        blocked_ids = left_ids.cross_join(right_ids)
        b = Blocking(dataset_pair, blocked_ids)
        return dataset_pair.scrub_redundant_comparisons(b)


class FunctionBlocker(PBlocker):
    """Blocks based on a function of type (Table, Table) -> Boolean"""

    def __init__(self, func):
        self.func = func

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
        left, right = dataset_pair
        # In case left and right are actually the same table, we need to take
        # a view so the self join works correctly.
        # https://ibis-project.org/user_guide/self_joins
        right = right.view()
        joined_full = left.join(
            right, predicates=self.func(left, right), how="inner", suffixes=("_l", "_r")
        )
        ids = joined_full["record_id_l", "record_id_r"]
        b = Blocking(dataset_pair, ids)
        return dataset_pair.scrub_redundant_comparisons(b)


def _join_tables(left: Table, right: Table, id_pairs: Table) -> Table:
    """Join two tables based on a table of (left_id, right_id) pairs."""
    if id_pairs.columns != ["record_id_l", "record_id_r"]:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
    left2 = left.relabel("{name}_l")
    right2 = right.relabel("{name}_r")
    return id_pairs.inner_join(left2, "record_id_l").inner_join(right2, "record_id_r")
