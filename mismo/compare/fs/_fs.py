from __future__ import annotations

import dataclasses
from typing import Callable, Iterable, Union

from ibis.expr.types import BooleanValue, Table

from mismo.block import PBlocking
from mismo.compare import Comparisons, PComparer


class FellegiSunterComparer(PComparer):
    def __init__(self, comparisons: Iterable[Comparison]):
        self.comparisons = list(comparisons)

    def compare(self, blocking: PBlocking) -> Comparisons:
        return Comparisons(blocking, blocking.blocked_data)


@dataclasses.dataclass(frozen=True)
class Comparison:
    name: str
    levels: Iterable[Condition]
    description: str | None = None


PredicateFunc = Callable[[Table], BooleanValue]
PredicateIsh = Union[PredicateFunc, BooleanValue]


@dataclasses.dataclass(frozen=True)
class Condition:
    name: str
    predicate: PredicateIsh
    description: str | None = None


@dataclasses.dataclass(frozen=True)
class Weights:
    m: float
    u: float


@dataclasses.dataclass(frozen=True)
class ComparisonLevel:
    condition: Condition
    weights: Weights | None = None
