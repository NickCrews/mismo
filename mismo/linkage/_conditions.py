from __future__ import annotations

from textwrap import dedent
from typing import Callable, Literal

import ibis
from ibis.expr import types as ir

from mismo import _typing, joins
from mismo.linkage._linkage import BaseLinkage
from mismo.linkage._linker import Linker, infer_task
from mismo.types import LinkedTable, LinksTable


class JoinConditionLinker(Linker):
    """
    A [Linker][mismo.Linker] that creates a [JoinConditionLinkage][mismo.JoinConditionLinkage] when given two tables.
    """  # noqa: E501

    def __init__(
        self,
        condition: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue],
        *,
        task: Literal["dedupe", "link"] | None = None,
        on_slow: Literal["error", "warn", "ignore"] = "error",
    ):
        self.condition = condition
        self.task = task
        self.on_slow = on_slow

    def __link__(self, left: ibis.Table, right: ibis.Table) -> JoinConditionLinkage:
        if left is right:
            right = right.view()
        # Run this to check early for slow joins
        self._get_pred(left, right)
        return JoinConditionLinkage(left, right, self.__join_condition__)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._get_pred(left, right)

    def _get_pred(self, left: ibis.Table, right: ibis.Table) -> ibis.ir.BooleanValue:
        task = infer_task(self.task, left, right)
        pred = joins.join_condition(self.condition).__join_condition__(left, right)
        if task == "dedupe":
            pred = pred & (left.record_id < right.record_id)
        joins.check_join_algorithm(left, right, pred, on_slow=self.on_slow)
        return pred


class JoinConditionLinkage(BaseLinkage):
    def __init__(
        self,
        left: ir.Table,
        right: ir.Table,
        join_condition: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue],
    ) -> None:
        self._join_condition = join_condition
        self._left_raw = left
        self._right_raw = right

    @property
    def left(self) -> LinkedTable:
        return LinkedTable.make_pair(
            left=self._left_raw, right=self._right_raw, links=self._links_raw
        )[0]

    @property
    def right(self) -> LinkedTable:
        return LinkedTable.make_pair(
            left=self._left_raw, right=self._right_raw, links=self._links_raw
        )[1]

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue]:
        return self._join_condition(left, right)

    @property
    def links(self):
        return LinksTable(
            self._links_raw,
            left=self._left_raw,
            right=self._right_raw,
        )

    @property
    def _links_raw(self):
        return joins.join(
            self._left_raw,
            self._right_raw,
            self._join_condition,
            lname="{name}_l",
            rname="{name}_r",
            rename_all=True,
        )

    def cache(self) -> _typing.Self:
        """
        No-op to cache this, since the condition is an abstract condition, and
        left and right are already cached.
        """
        # I think this API is fine to just return self instead of a new instance?
        return self

    def __repr__(self) -> str:
        return dedent(
            f"""
            {self.__class__.__name__}(
                nleft={self.left.count().execute():_}
                nright={self.right.count().execute():_}
                nlinks={self.links.count().execute():_}
            )""".strip()
        )
