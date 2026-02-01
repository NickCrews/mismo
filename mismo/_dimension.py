from __future__ import annotations

from collections.abc import Iterable
import typing

import ibis
from ibis import ir

from mismo.types._links_table import LinksTable


@typing.runtime_checkable
class Dimension(typing.Protocol):
    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue | bool:
        pass

    def __compare__(self, links: LinksTable) -> LinksTable:
        pass


class DimensionsLinker:
    def __init__(self, dimensions: Iterable[Dimension]) -> None:
        self.dimension = tuple(dimensions)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue | bool:
        clauses = [linker.__join_condition__(left, right) for linker in self.dimension]
        clauses = [clause for clause in clauses if clause is not True]
        return ibis.and_(*clauses)

    def __compare__(self, links: LinksTable) -> LinksTable:
        for linker in self.dimension:
            links = linker.__compare__(links)
        return links
