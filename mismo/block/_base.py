from __future__ import annotations

from functools import cache
from textwrap import dedent
from typing import Callable, Iterable, Literal, Union

import ibis
from ibis.expr.types import BooleanValue, Table

from mismo import _util
from mismo._util import format_table


class Blocking:
    _left: Table
    _right: Table
    _blocked_ids: Table
    _blocked_data: Table

    def __init__(
        self,
        left: Table,
        right: Table,
        *,  # force keyword arguments
        blocked_ids: Table | None = None,
        blocked_data: Table | None = None,
    ) -> None:
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

            blocked_data = _join_on_ids(left, right, blocked_ids)
        else:
            blocked_ids = blocked_data["record_id_l", "record_id_r"]
        self._left = left
        self._right = right
        self._blocked_ids = blocked_ids
        self._blocked_data = _order_blocked_data_columns(blocked_data)

    @property
    def left(self) -> Table:
        """The left Table"""
        return self._left

    @property
    def right(self) -> Table:
        """The right Table"""
        return self._right

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
        return hash((self.left, self.right, self.blocked_ids))


_JOIN_CONDITON = Union[BooleanValue, Literal[True]]
BlockingRule = Union[
    _JOIN_CONDITON,
    Callable[[Table, Table], _JOIN_CONDITON],
    Callable[[Table, Table], Table],
]


def block(
    left: Table,
    right: Table,
    rules: Iterable[BlockingRule],
    skip_rules: Iterable[BlockingRule],
) -> Blocking:
    r = ibis.util.promote_list(rules)
    sr = ibis.util.promote_list(skip_rules)
    raw = _block(left, right, r, sr)
    return raw


def cartesian_block(left: Table, right: Table) -> Blocking:
    return block(left, right, True, [])


def _block(left: Table, right: Table, blocker: BlockingRule) -> Blocking:
    if isinstance(blocker, list):
        ids_chunks = [
            _util.join(left, right, rule)["record_id_l", "record_id_r"]
            for rule in blocker
        ]
        return Blocking(
            left,
            blocked_ids=ibis.union(*ids_chunks, distinct=True),
        )
    elif isinstance(blocker, BooleanValue) or blocker is True:
        return _block(left, [blocker])
    else:
        func_result = blocker(left, right)
        return _block(left, func_result)


def _join_on_ids(left: Table, right: Table, id_pairs: Table) -> Table:
    """Join two tables based on a table of (record_id_l, record_id_r) pairs."""
    if set(id_pairs.columns) != {"record_id_l", "record_id_r"}:
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
