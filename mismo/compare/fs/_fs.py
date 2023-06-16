from __future__ import annotations

import dataclasses
import math
from typing import Callable, Iterable, Protocol, Self

import ibis
from ibis.expr.types import BooleanValue, IntegerColumn, Table

from mismo.block import PBlocking
from mismo.compare import Comparisons, PComparer


class FellegiSunterComparer(PComparer):
    def __init__(self, comparisons: Iterable[Comparison], prior: float | None = None):
        self.comparisons = list(comparisons)
        # The probability of a match between two records drawn at random.
        # Equivalent to probability_two_random_records_match from splink.
        self.prior = prior

    def compare(self, blocking: PBlocking) -> Comparisons:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class Comparison:
    name: str
    levels: list[PComparisonLevel]
    description: str | None = None

    def label_pairs(self, blocked_data: Table) -> IntegerColumn:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it
        with the level's index. If it doesn't match any level, it has the label NA.

        Equivalent to the lambda values described at
        https://www.robinlinacre.com/maths_of_fellegi_sunter/"""
        result = ibis.NA
        for i, level in enumerate(self.levels):
            is_match = result.isnull() & level.predicate(blocked_data)
            result = is_match.ifelse(i, result)
        return result.name(f"{self.name}_level")


class PComparisonLevel(Protocol):
    name: str
    predicate: Callable[[Table], BooleanValue]
    description: str | None = None
    weights: Weights | None = None

    def set_weights(self, weights: Weights) -> Self:
        """Return a version of this level with the given weights."""
        ...


@dataclasses.dataclass(frozen=True)
class ComparisonLevel(PComparisonLevel):
    name: str
    predicate: Callable[[Table], BooleanValue]
    description: str | None = None
    weights: Weights | None = None

    def set_weights(self, weights: Weights) -> Self:
        """Return a version of this level with the given weights."""
        return dataclasses.replace(self, weights=weights)


class Weights:
    def __init__(self, *, m: float, u: float):
        self.m = m
        self.u = u

    @property
    def bayes_factor(self) -> float:
        if self.u == 0:
            return float("inf")
        else:
            return self.m / self.u

    @property
    def log2_bayes_factor(self):
        return math.log2(self.bayes_factor)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(m={self.m}, u={self.u})"
