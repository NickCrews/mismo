from __future__ import annotations

from typing import Any, Literal, Protocol, runtime_checkable

import ibis

from mismo import joins
from mismo.linkage._conditions import JoinConditionLinkage
from mismo.linkage._linkage import Linkage


@runtime_checkable
class Linker(Protocol):
    """
    A Protocol that takes two tables of records and produces a [Linkage][mismo.Linkage].
    """

    def __link__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"],
    ) -> Linkage:
        raise NotImplementedError


class FullLinker:
    """
    A Linker that fully joins two tables, yielding all possible pairs (MxN of them).
    """

    def __link__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"],
    ) -> Linkage:
        return link(left, right, True, on_slow="ignore", task=task)


class EmptyLinker:
    """A Linker that yields no pairs."""

    def __link__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"],
    ) -> Linkage:
        return link(left, right, False, task=task)


class UnnestLinker:
    def __init__(self, column: str):
        self.column = column

    def __link__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"],
    ) -> Linkage:
        left = left.mutate(left[self.column].unnest().name(self.column))
        right = left.mutate(right[self.column].unnest().name(self.column))
        return link(left, right, self.column, task=task)


def link(
    left: ibis.Table,
    right: ibis.Table,
    linker: Any | Linker,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
) -> Linkage:
    """Create a Linkage from the given tables of records and linker

    This is a wrapper around `ibis.join` that

    - Allows for slightly more flexible join conditions.
    - Renames the columns in the resulting table to have "_l" and "_r" suffixes,
      to be consistent with Mismo's conventions.
    - Sorts the output columns into a nicer order.
    - Checks if the join condition is likely to cause a slow O(n*m) join algorithm.
    - If `task` is "dedupe", adds an additional restriction that
      `record_id_l < record_id_r` to remove duplicates.

    Parameters
    ----------
    left
        The left table to block
    right
        The right table to block
    linker
        Either something that satisfies the [Linker][Linker] protocol,
        or anything that `mismo.join()` accepts as a join predicate.
    on_slow
        What to do if the join condition causes a slow O(n*m) join algorithm.
        If "error", raise a SlowJoinError.
        If "warn", issue a SlowJoinWarning.
        If "ignore", do nothing.
        See [check_join_algorithm()][mismo.block.check_join_algorithm] for more information.
    task
        If "dedupe", the resulting pairs will have the additional restriction that
        `record_id_l < record_id_r`.
        If "link", no additional restriction is added.
        If None, will be inferred by looking at left and right:
        - If `id(left) == id(right) or left.equals(right)`, then "dedupe" is assumed.
        - Otherwise, "link" is assumed.
    """  # noqa: E501
    # if you do link(t.drop("col"), t.drop("col")), you probably want to be doing
    # deduplication. So if tables are *structurally* the same, assume dedupe.
    from mismo.linkage._key_linker import KeyLinkage

    if id(left) == id(right) or left.equals(right):
        right = right.view()
        if task is None:
            task = "dedupe"
    if isinstance(linker, Linker):
        return linker.__link__(left, right, task=task)
    condition = joins.join_condition(linker)
    if isinstance(condition, joins.KeyJoinCondition):
        return KeyLinkage((condition.left_spec, condition.right_spec))
    elif isinstance(condition, joins.MultiKeyJoinCondition):
        keys = [(sub.left_spec, sub.right_spec) for sub in condition.subconditions]
        return KeyLinkage(*keys)
    else:
        return JoinConditionLinkage(condition, on_slow=on_slow)
