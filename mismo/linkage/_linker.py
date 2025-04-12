from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

import ibis

from mismo.linkage import _conditions, _linkage


@runtime_checkable
class Linker(Protocol):
    """
    A Protocol that takes two tables of records and produces a [Linkage][mismo.Linkage].
    """

    def __link__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        """Given two tables, return a Linkage."""
        raise NotImplementedError


class FullLinker(Linker):
    """
    A Linker that fully joins two tables, yielding all possible pairs (MxN of them).
    """

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _conditions.JoinConditionLinker(
            True, on_slow="ignore", task=task
        )

    def __link__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker.__link__(left, right)


class EmptyLinker(Linker):
    """A Linker that yields no pairs."""

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _conditions.JoinConditionLinker(
            False, on_slow="ignore", task=task
        )

    def __link__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker.__link__(left, right)


class UnnestLinker(Linker):
    """A Linker that unnests a column before linking."""

    def __init__(self, column: str, *, task: Literal["dedupe", "link"] | None = None):
        self.column = column
        self.task = task
        self._linker = _conditions.JoinConditionLinker(self.column, task=task)

    def __link__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        left = left.mutate(left[self.column].unnest().name(self.column))
        right = left.mutate(right[self.column].unnest().name(self.column))
        return self._linker.__link__(left, right)


class _LinkerLinkage(_linkage.BaseLinkage):
    _linker_cls: type[Linker]

    def __init__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ):
        self.task = task
        self._linkage = self._linker_cls(task=task).__link__(left, right)

    @property
    def left(self) -> ibis.Table:
        return self._linkage.left

    @property
    def right(self) -> ibis.Table:
        return self._linkage.right

    @property
    def links(self) -> ibis.Table:
        return self._linkage.links

    def cache(self):
        return self


class FullLinkage(_LinkerLinkage):
    """
    A Linkage that fully joins two tables, yielding all possible pairs (MxN of them).
    """

    _linker_cls = FullLinker


class EmptyLinkage(_LinkerLinkage):
    """A Linkage that yields no pairs."""

    _linker_cls = EmptyLinker


class UnnestLinkage(_LinkerLinkage):
    """A Linkage that unnests a column before linking."""

    _linker_cls = UnnestLinker


def infer_task(
    task: Literal["dedupe", "link"] | None, left: ibis.Table, right: ibis.Table
) -> Literal["dedupe", "link"]:
    if task is None:
        task = "dedupe" if left is right else "link"
    return task
