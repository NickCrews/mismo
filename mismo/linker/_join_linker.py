from __future__ import annotations

from typing import Callable, Literal

import ibis
from ibis.expr import types as ir

from mismo import _typing, joins
from mismo.linkage._linkage import BaseLinkage
from mismo.linker._linker import Linker, infer_task
from mismo.types import LinkedTable, LinksTable


class JoinLinker(Linker):
    """
    A [Linker][mismo.Linker] that creates a [JoinLinkage][mismo.JoinLinkage] based on a join condition.
    """  # noqa: E501

    def __init__(
        self,
        condition: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue],
        *,
        task: Literal["dedupe", "link"] | None = None,
        on_slow: Literal["error", "warn", "ignore"] = "error",
    ):
        self.condition = joins.join_condition(condition)
        self.task = task
        self.on_slow = on_slow

    def __call__(self, left: ibis.Table, right: ibis.Table) -> JoinLinkage:
        if left is right:
            right = right.view()
        # Run this to check early for slow joins
        self._get_pred(left, right)
        return JoinLinkage(left, right, self.__join_condition__)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._get_pred(left, right)

    def _get_pred(self, left: ibis.Table, right: ibis.Table) -> ibis.ir.BooleanValue:
        task = infer_task(self.task, left, right)
        pred = self.condition.__join_condition__(left, right)
        if task == "dedupe":
            pred = pred & (left.record_id < right.record_id)
        joins.check_join_algorithm(left, right, pred, on_slow=self.on_slow)
        return pred

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<condition={self.condition!r}, task={self.task}, on_slow={self.on_slow}>"  # noqa: E501


class JoinLinkage(BaseLinkage):
    """A [Linkage][mismo.Linkage] based on a join condition."""

    def __init__(
        self,
        left: ir.Table,
        right: ir.Table,
        condition: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue],
    ) -> None:
        self.condition = joins.join_condition(condition)
        if left is right:
            right = right.view()
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
        return self.condition.__join_condition__(left, right)

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
            self.condition,
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
        return f"{self.__class__.__name__}<nleft={self.left.count().execute():_}, nright={self.right.count().execute():_}, nlinks={self.links.count().execute():_}>"  # noqa: E501
