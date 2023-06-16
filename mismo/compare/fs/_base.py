from __future__ import annotations

import dataclasses
import math
from typing import Callable, Iterable

import ibis
from ibis.expr.types import (
    BooleanValue,
    FloatingColumn,
    FloatingValue,
    IntegerColumn,
    Table,
)

from mismo._dataset import PDatasetPair
from mismo._typing import Self
from mismo.block import PBlocking
from mismo.compare import Comparisons

from ._util import prob_to_bayes_factor


class FellegiSunterComparer:
    def __init__(self, comparisons: Iterable[Comparison], prior: float | None = None):
        self.comparisons = list(comparisons)
        # The probability of a match between two records drawn at random.
        # Equivalent to probability_two_random_records_match from splink.
        self.prior = prior

    def compare(self, blocking: PBlocking) -> Comparisons:
        if not self.is_trained:
            raise ValueError(f"{self} is not trained.")
        pairs = blocking.blocked_data
        bf = self._bayes_factor(pairs)
        return Comparisons(blocking=blocking, compared=pairs.mutate(bayes_factor=bf))

    def trained(
        self,
        dataset_pair: PDatasetPair,
        max_pairs: int | None = None,
        seed: int | None = None,
    ) -> Self:
        """
        Return a new version of this comparer that is trained on the given dataset pair.
        """
        from ._train import train_comparison

        trained = [
            train_comparison(c, dataset_pair, max_pairs=max_pairs, seed=seed)
            for c in self.comparisons
        ]
        return self.__class__(trained, prior=self.prior)

    @property
    def is_trained(self) -> bool:
        return self.prior is not None and all(c.is_trained for c in self.comparisons)

    def _bayes_factor(self, pairs: Table) -> FloatingValue:
        if self.prior == 1.0:
            return ibis.literal(float("inf"))  # type: ignore
        bf = prob_to_bayes_factor(self.prior)  # type: ignore
        for comparison in self.comparisons:
            bf *= comparison.bayes_factor(pairs)  # type: ignore
        return bf  # type: ignore


@dataclasses.dataclass(frozen=True)
class Comparison:
    name: str
    levels: list[ComparisonLevel]
    description: str | None = None

    def label_pairs(self, pairs: Table) -> IntegerColumn:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it
        with the level's index. If it doesn't match any level, it has the label NA.
        This would be the ELSE case that is used in splink, but in our version
        we don't explicitly have an ELSE ComparisonLevel.

        Equivalent to the gamma values described at
        https://www.robinlinacre.com/maths_of_fellegi_sunter/"""
        result = ibis.NA
        for i, level in enumerate(self.levels):
            is_match = result.isnull() & level.predicate(pairs)
            result = is_match.ifelse(i, result)
        return result.name(f"{self.name}_level")  # type: ignore

    def bayes_factor(self, pairs: Table) -> FloatingColumn:
        """Calculate the Bayes factor for each record pair."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        gammas = self.label_pairs(pairs)
        cases = [(i, level.weights.bayes_factor) for i, level in enumerate(self.levels)]  # type: ignore # noqa: E501
        return gammas.cases(cases, self.else_level.weights.bayes_factor)  # type: ignore

    @property
    def else_level(self) -> ComparisonLevel:
        """A level that matches all record pairs that don't match any other level."""
        if self.is_trained:
            ms = [level.weights.m for level in self.levels]  # type: ignore
            us = [level.weights.u for level in self.levels]  # type: ignore
            else_m = 1 - sum(ms)
            else_u = 1 - sum(us)
            weights = Weights(m=else_m, u=else_u)
        else:
            weights = None
        return ComparisonLevel(
            name="else",
            predicate=lambda _: ibis.literal(True),  # type: ignore
            description="Else",
            weights=weights,
        )

    @property
    def is_trained(self) -> bool:
        return all(level.weights is not None for level in self.levels)


@dataclasses.dataclass(frozen=True)
class ComparisonLevel:
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
