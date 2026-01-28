from __future__ import annotations

from typing import Literal

import ibis

from mismo.linkage import _linkage
from mismo.linker import _common, _join_linker


class FullLinker(_common.Linker):
    """
    A [Linker][mismo.Linker] that yields all possible pairs (MxN of them).
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


class EmptyLinker(_common.Linker):
    """A [Linker][mismo.Linker] that yields no pairs."""

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _join_linker.JoinLinker(False, on_slow="ignore", task=task)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._linker.__join_condition__(left, right)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker(left, right)
