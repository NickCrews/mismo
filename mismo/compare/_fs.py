"""A Comparer that uses the Fellegi-Sunter model to score pairs of records."""

from __future__ import annotations

import dataclasses
from typing import Callable, Iterable, Union

from ibis.expr.types import BooleanColumn, Table

from mismo.block import PBlocking
from mismo.compare import Comparisons, PComparer


class FellegiSunterComparer(PComparer):
    def __init__(self, comparisons: Iterable[FSComparison]):
        self.comparisons = comparisons

    def compare(self, blocking: PBlocking) -> Comparisons:
        return Comparisons(blocking, blocking.blocked_data)


@dataclasses.dataclass(frozen=True)
class FSComparison:
    name: str
    levels: Iterable[FSComparisonLevel]
    description: str | None = None


ConditionFunc = Callable[[Table], BooleanColumn]
ConditionIsh = Union[ConditionFunc, BooleanColumn]


@dataclasses.dataclass(frozen=True)
class FSComparisonLevel:
    name: str
    condition: ConditionIsh
    description: str | None = None
