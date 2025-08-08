from __future__ import annotations

from typing import Literal

import ibis

from mismo.linkage import _linkage
from mismo.linker import _common, _join_linker


class FullLinker(_common.Linker):
    """
    A [Linker][mismo.Linker] that yields all possible pairs.

    This will be N x M pairs for linking tasks,
    and N x (M-1) pairs for deduplication tasks.
    """

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _join_linker.JoinLinker(True, on_slow="ignore", task=task)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._linker.__join_condition__(left, right)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker(left, right)


def full_linkage(
    left: ibis.Table,
    right: ibis.Table,
    *,
    task: Literal["dedupe", "link"] | None = None,
) -> _linkage.Linkage:
    """
    Create a linkage with all (M x N) possible pairs of the two tables.

    Parameters
    ----------
    left
        The left table to link.
    right
        The right table to link.
    task
        The task type, either "dedupe" or "link".
        If None, will be inferred from whether the left and right tables are the same.
    """
    return FullLinker(task=task)(left, right)


class EmptyLinker(_common.Linker):
    """A [Linker][mismo.Linker] that yields no pairs."""

    def __init__(self):
        # The task doesn't matter here, since we won't be linking anything.
        self._linker = _join_linker.JoinLinker(False, on_slow="ignore", task="link")

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._linker.__join_condition__(left, right)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker(left, right)


def empty_linkage(left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
    """
    Create a Linkage with no pairs. This is useful for testing or as a placeholder.

    Parameters
    ----------
    left
        The left table to link.
    right
        The right table to link.
    """
    return EmptyLinker()(left, right)


class UnnestLinker(_common.Linker):
    """A [Linker][mismo.Linker] that unnests a column before linking."""

    def __init__(self, column: str, *, task: Literal["dedupe", "link"] | None = None):
        self.column = column
        self.task = task
        self._linker = _join_linker.JoinLinker(self.column, task=task)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        left = left.mutate(left[self.column].unnest().name(self.column))
        right = left.mutate(right[self.column].unnest().name(self.column))
        return self._linker.__call__(left, right)
