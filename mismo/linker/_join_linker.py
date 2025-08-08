from __future__ import annotations

from typing import Callable, Literal

import ibis

from mismo import joins
from mismo.linkage._linkage import Linkage
from mismo.linker._common import Linker, infer_task
from mismo.types import LinksTable


class JoinLinker(Linker):
    """
    A [Linker][mismo.Linker] based on a join condition.
    """  # noqa: E501

    def __init__(
        self,
        condition: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue],
        *,
        task: Literal["dedupe", "link"] | None = None,
        on_slow: Literal["error", "warn", "ignore"] = "error",
    ) -> None:
        """Create from a join condition.

        Parameters
        ----------
        condition
            The join condition.
        task
            The task to perform. If `None`, the task will be inferred as
            "dedupe" if the two tables passed to __call__ are the same,
            otherwise it will be inferred as "link".
        on_slow
            What to do if the join is slow. See [mismo.joins.check_join_algorithm][].
        """
        self.condition = joins.join_condition(condition)
        self.task = task
        self.on_slow = on_slow

    def __call__(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        """Create a linkage from the join condition."""
        task = infer_task(task=self.task, left=left, right=right)
        if left is right:
            right = right.view()
        # Run this to check early for slow joins
        self.__join_condition__(left, right)
        condition = self.__join_condition__(left, right)
        if task == "dedupe":
            condition = condition & (left.record_id < right.record_id)
        links = LinksTable.from_join_condition(left, right, condition)
        return Linkage(left=left, right=right, links=links)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        """Create a join condition."""
        pred = self.condition.__join_condition__(left, right)
        joins.check_join_algorithm(left, right, pred, on_slow=self.on_slow)
        return pred

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<condition={self.condition!r}, task={self.task}, on_slow={self.on_slow}>"  # noqa: E501
