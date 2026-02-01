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

    def __join_condition__(self, left: ibis.Table, right: ibis.Table) -> Literal[True]:
        return True

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return _join_linker.JoinLinker(True, on_slow="ignore", task=self.task)(
            left, right
        )


class EmptyLinker(_common.Linker):
    """A [Linker][mismo.Linker] that yields no pairs."""

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task

    def __join_condition__(self, left: ibis.Table, right: ibis.Table) -> Literal[False]:
        return False

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return _join_linker.JoinLinker(False, on_slow="ignore", task=self.task)(
            left, right
        )
