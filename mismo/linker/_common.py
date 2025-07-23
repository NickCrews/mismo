from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

import ibis

from mismo.linkage import _linkage


@runtime_checkable
class Linker(Protocol):
    """
    A Protocol that takes two tables of records and produces a [Linkage][mismo.Linkage].
    """

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        """Given two tables, return a Linkage."""
        raise NotImplementedError


def infer_task(
    *, task: Literal["dedupe", "link"] | None, left: ibis.Table, right: ibis.Table
) -> Literal["dedupe", "link"]:
    if task is not None:
        return task
    if left.equals(right):
        return "dedupe"
    return "link"
