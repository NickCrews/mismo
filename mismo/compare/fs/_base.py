from __future__ import annotations

import dataclasses
import math
from typing import Iterable

import ibis
from ibis.expr.types import FloatingColumn, IntegerColumn, Table

from mismo._typing import Self
from mismo.compare._comparison import Comparison

from ._util import bayes_factor_to_prob, prob_to_bayes_factor


class LevelWeights:
    def __init__(self, name: str, *, m: float | None, u: float | None):
        self.name = name
        self.m = m
        self.u = u

    @property
    def is_trained(self) -> bool:
        return self.m is not None and self.u is not None

    @property
    def bayes_factor(self) -> float:
        if not self.is_trained:
            raise ValueError("Weights have not been set for this comparison level.")
        if self.u == 0:
            return float("inf")
        else:
            return self.m / self.u

    @property
    def log2_bayes_factor(self):
        return math.log2(self.bayes_factor)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, m={self.m}, u={self.u})"


@dataclasses.dataclass(frozen=True)
class ComparisonWeights:
    name: str
    """Matches the name of the Comparison that these weights are for."""
    level_weights: list[LevelWeights]

    @classmethod
    def from_comparison(cls, comparison: Comparison) -> ComparisonWeights:
        return cls(
            name=comparison.name,
            level_weights=[
                LevelWeights(name=lev.name, m=None, u=None) for lev in comparison.levels
            ],
        )

    @property
    def is_trained(self) -> bool:
        return all(w.is_trained for w in self.level_weights)

    def bayes_factor(self, labels: IntegerColumn) -> FloatingColumn:
        """Calculate the Bayes factor for each record pair."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        cases = [(i, w.bayes_factor) for i, w in enumerate(self.level_weights)]  # type: ignore # noqa: E501
        return labels.cases(cases, self.else_weights.bayes_factor)  # type: ignore

    def match_probability(self, pairs_or_labels: IntegerColumn) -> FloatingColumn:
        """Calculate the match probability for each record pair."""
        bf = self.bayes_factor(pairs_or_labels)
        return bayes_factor_to_prob(bf)

    @property
    def else_weights(self) -> LevelWeights:
        """A level that matches all record pairs that don't match any other level."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        ms = [w.m for w in self.level_weights]  # type: ignore
        us = [w.u for w in self.level_weights]  # type: ignore
        else_m = 1 - sum(ms)
        else_u = 1 - sum(us)
        return LevelWeights(name="else", m=else_m, u=else_u)


class Weights:
    """
    Weights for the Fellegi-Sunter model.

    Attributes
    ----------
    prior : float
        The probability of a match between two records drawn at random.
        Equivalent to probability_two_random_records_match from splink.
    """

    def __init__(
        self, *, comparison_weights: Iterable[ComparisonWeights], prior: float | None
    ):
        self.comparison_weights = {cw.name: cw for cw in comparison_weights}
        self.prior = prior

    @classmethod
    def from_comparisons(
        cls, comparisons: Iterable[Comparison], prior: float | None = None
    ) -> Weights:
        comparison_weights = [ComparisonWeights.from_comparison(c) for c in comparisons]
        return cls(comparison_weights=comparison_weights, prior=prior)

    def __getitem__(self, name: str) -> ComparisonWeights:
        return self.comparison_weights[name]

    @property
    def is_trained(self) -> bool:
        return (
            all(cw.is_trained for cw in self.comparison_weights.values())
            and self.prior is not None
        )


class FellegiSunterComparer:
    def __init__(
        self, comparisons: Iterable[Comparison], weights: Weights | None = None
    ):
        self.comparisons: list[Comparison] = ibis.util.promote_list(comparisons)
        if weights is None:
            weights = Weights.from_comparisons(comparisons)
        self.weights = weights

    def compare(self, blocked: Table) -> Table:
        if not self.is_trained:
            raise ValueError(f"{self} is not trained.")

        total_bf = prob_to_bayes_factor(self.weights.prior)
        m = {}
        for comparison in self.comparisons:
            comparison_weights = self.weights[comparison.name]
            labels = comparison.label_pairs(blocked, label="index")
            bf = comparison_weights.bayes_factor(labels)
            m[f"{comparison.name}_cmp"] = comparison.label_pairs(blocked, label="name")
            m[f"{comparison.name}_bf"] = bf
            total_bf *= bf
        if self.weights == 1.0:
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
        weights = Weights(
            comparison_weights=trained,
            prior=self.weights.prior if self.weights else None,
        )
        return self.__class__(comparisons=self.comparisons, weights=weights)

    @property
    def is_trained(self) -> bool:
        return self.weights.is_trained

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.comparisons!r}, {self.weights!r})"
