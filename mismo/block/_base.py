from __future__ import annotations

from functools import cache
from textwrap import dedent
from typing import Callable, Literal, Union

import ibis
from ibis.expr.types import BooleanValue, Table

from mismo import _util
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

            left, right = self.dataset_pair
            blocked_data = _join_on_ids(left, right, blocked_ids)
        else:
            blocked_ids = blocked_data["record_id_l", "record_id_r"]
        self._blocked_ids = blocked_ids
        self._blocked_data = _order_blocked_data_columns(blocked_data)

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


_Blocker = Union[
    Literal[True],
    BooleanValue,
    list[BooleanValue],
    Callable[[PDatasetPair], BooleanValue],
    Callable[[PDatasetPair], BooleanValue],
    Callable[[Table, Table], list[BooleanValue]],
    Callable[[Table, Table], list[BooleanValue]],
]

Blocker = Union[_Blocker, list[_Blocker]]


def block(dataset_pair: PDatasetPair, blocker: Blocker) -> Blocking:
    raw = _block(dataset_pair, blocker)
    return dataset_pair.scrub_redundant_comparisons(raw)


def cartesian_block(dataset_pair: PDatasetPair) -> Blocking:
    return block(dataset_pair, True)


def _block(dataset_pair: PDatasetPair, blocker: Blocker) -> Blocking:
    if isinstance(blocker, list):
        left, right = dataset_pair
        ids_chunks = [
            _util.join(left, right, rule)["record_id_l", "record_id_r"]
            for rule in blocker
        ]
        return Blocking(
            dataset_pair,
            blocked_ids=ibis.union(*ids_chunks, distinct=True),
        )
    elif isinstance(blocker, BooleanValue) or blocker is True:
        return _block(dataset_pair, [blocker])
    else:
        try:
            func_result = blocker(dataset_pair)
        except TypeError:
            left, right = dataset_pair
            func_result = blocker(left, right)
        return _block(dataset_pair, func_result)


def _join_on_ids(left: Table, right: Table, id_pairs: Table) -> Table:
    """Join two tables based on a table of (left_id, right_id) pairs."""
    if id_pairs.columns != ["record_id_l", "record_id_r"]:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
    left2 = left.relabel("{name}_l")
    right2 = right.relabel("{name}_r")
    return id_pairs.inner_join(left2, "record_id_l").inner_join(right2, "record_id_r")


def _order_blocked_data_columns(t: Table) -> Table:
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]
