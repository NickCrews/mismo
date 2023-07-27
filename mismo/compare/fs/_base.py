from __future__ import annotations

import dataclasses
import math
from typing import Iterable, Literal

import ibis
from ibis.expr.types import FloatingColumn, IntegerColumn, Table

from mismo._typing import Self
from mismo.compare._comparison import Comparison, ComparisonLevel

from ._util import bayes_factor_to_prob, prob_to_bayes_factor


@dataclasses.dataclass(frozen=True)
class FSComparison:
    comparison: Comparison
    weights: list[Weights] | None = None

    @property
    def name(self) -> str:
        return self.comparison.name

    @property
    def description(self) -> str | None:
        return self.comparison.description

    @property
    def levels(self) -> list[ComparisonLevel]:
        return self.comparison.levels

    def label_pairs(
        self, pairs: Table, label: Literal["index"] | Literal["name"] = "index"
    ) -> IntegerColumn:
        return self.comparison.label_pairs(pairs, label=label)

    def bayes_factor(self, pairs_or_labels: Table | IntegerColumn) -> FloatingColumn:
        """Calculate the Bayes factor for each record pair."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        if isinstance(pairs_or_labels, IntegerColumn):
            gammas = pairs_or_labels
        else:
            gammas = self.label_pairs(pairs_or_labels)
        cases = [(i, w.bayes_factor) for i, w in enumerate(self.weights)]  # type: ignore # noqa: E501
        return gammas.cases(cases, self.else_weights.bayes_factor)  # type: ignore

    def match_probability(
        self, pairs_or_labels: Table | IntegerColumn
    ) -> FloatingColumn:
        """Calculate the match probability for each record pair."""
        bf = self.bayes_factor(pairs_or_labels)
        return bayes_factor_to_prob(bf)

    @property
    def is_trained(self) -> bool:
        return self.weights is not None

    @property
    def else_weights(self) -> Weights:
        """A level that matches all record pairs that don't match any other level."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        ms = [w.m for w in self.weights]  # type: ignore
        us = [w.u for w in self.weights]  # type: ignore
        else_m = 1 - sum(ms)
        else_u = 1 - sum(us)
        return Weights(m=else_m, u=else_u)


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


class FellegiSunterComparer:
    def __init__(self, comparisons: Iterable[Comparison], prior: float | None = None):
        comparisons = ibis.util.promote_list(comparisons)
        self.comparisons = [
            c if isinstance(c, FSComparison) else FSComparison(c) for c in comparisons
        ]
        # The probability of a match between two records drawn at random.
        # Equivalent to probability_two_random_records_match from splink.
        self.prior = prior

    def compare(self, blocked: Table) -> Table:
        if not self.is_trained:
            raise ValueError(f"{self} is not trained.")

        total_bf = prob_to_bayes_factor(self.prior)
        m = {}
        for comparison in self.comparisons:
            bf = comparison.bayes_factor(blocked)
            m[f"{comparison.name}_cmp"] = comparison.label_pairs(blocked, label="name")
            m[f"{comparison.name}_bf"] = bf
            total_bf *= bf
        if self.prior == 1.0:
            total_bf = ibis.literal(float("inf"))
        m["bf"] = total_bf
        return blocked.mutate(**m)

    def trained(
        self,
        left: Table,
        right: Table,
        max_pairs: int | None = None,
        seed: int | None = None,
    ) -> Self:
        """
        Return a new version of this comparer that is trained on the given dataset pair.
        """
        from ._train import train_comparison

        trained = [
            train_comparison(fsc, left, right, max_pairs=max_pairs, seed=seed)
            for fsc in self.comparisons
        ]
        return self.__class__(trained, prior=self.prior)

    @property
    def is_trained(self) -> bool:
        return self.prior is not None and all(c.is_trained for c in self.comparisons)
