from __future__ import annotations

import dataclasses
import math
from typing import Callable, Iterable

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
    levels: list[ComparisonLevel]
    description: str | None = None


@dataclasses.dataclass(frozen=True)
class Condition:
    name: str
    predicate: Callable[[Table], BooleanValue]
    description: str | None = None


@dataclasses.dataclass(frozen=True)
class Weights:
    m: float
    u: float

    @property
    def bayes_factor(self) -> float:
        if self.u == 0:
            return float("inf")
        else:
            return self.m / self.u

    @property
    def log2_bayes_factor(self):
        return math.log2(self.bayes_factor)


@dataclasses.dataclass(frozen=True)
class ComparisonLevel:
    condition: Condition
    weights: Weights | None = None
