from __future__ import annotations

import dataclasses
import math
from typing import Iterable

import ibis
from ibis.expr.types import FloatingValue, IntegerValue, StringValue, Table

from mismo._typing import Self
from mismo.compare._comparison import Comparison, Comparisons

from ._util import bayes_factor_to_prob, prob_to_bayes_factor


class LevelWeights:
    """Weights for a single level of a Comparison.

    This describes for example "If zipcodes match perfectly, then
    this increases the probability of a match by 10x as compared to if we
    hadn't looked at zipcode".
    """

    name: str
    """The name of the level, e.g. "Exact Match"."""
    m: float | None
    """Among true-matches, what proportion of them have this level?

    1 means this level is a good indication of a match, 0 means it's a good
    indication of a non-match.
    """
    u: float | None
    """Among non-matches, what proportion of them have this level?

    1 means this level is a good indication of a non-match, 0 means it's a good
    indication of a match.
    """

    def __init__(self, name: str, *, m: float | None, u: float | None):
        """Create a new LevelWeights object."""
        self.name = name
        self.m = m
        self.u = u

    @property
    def is_trained(self) -> bool:
        """If m and u have been set."""
        return self.m is not None and self.u is not None

    @property
    def bayes_factor(self) -> float:
        """How much more likely is a match than a non-match at this level?

        This is derived from m and u.

        Similar to the concept of odds.
        - values below 1 is evidence against a match
        - values above 1 is evidence for a match
        - 1 means this level does not provide any evidence for or against a match
        """
        if not self.is_trained:
            raise ValueError("Weights have not been set for this comparison level.")
        if self.u == 0:
            return float("inf")
        else:
            return self.m / self.u

    @property
    def log2_bayes_factor(self):
        """log2 of the bayes factor."""
        return math.log2(self.bayes_factor)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, m={self.m}, u={self.u})"


@dataclasses.dataclass(frozen=True)
class ComparisonWeights:
    name: str
    """The name of the Comparison that these weights are for, eg "name" or "address"."""
    level_weights: list[LevelWeights]
    """The weights for each level of the Comparison."""

    @classmethod
    def from_comparison(cls, comparison: Comparison) -> ComparisonWeights:
        """Create untrained weights from a Comparison."""
        return cls(
            name=comparison.name,
            level_weights=[
                LevelWeights(name=lev.name, m=None, u=None) for lev in comparison.levels
            ],
        )

    @property
    def is_trained(self) -> bool:
        """If all level weights have been set."""
        return all(w.is_trained for w in self.level_weights)

    def bayes_factor(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the Bayes factor for each record pair."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        if isinstance(labels, StringValue):
            cases = [(lw.name, lw.bayes_factor) for lw in self.level_weights]
        else:
            cases = [(i, lw.bayes_factor) for i, lw in enumerate(self.level_weights)]  # type: ignore # noqa: E501
        return labels.cases(cases, self.else_weights.bayes_factor)  # type: ignore

    def match_probability(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the match probability for each record pair."""
        return bayes_factor_to_prob(self.bayes_factor(labels))

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
    """Weights for the Fellegi-Sunter model.

    Can either be trained or untrained. Comprised of
    - a `prior` probability, which is the probability of a match between two
      records drawn at random.
    - a set of `ComparisonWeights`, one for each Comparison."""

    comparison_weights: dict[str, ComparisonWeights]
    """The weights for each Comparison."""

    prior: float | None
    """The probability of a match between two records drawn at random.

    Equivalent to probability_two_random_records_match from splink."""

    def __init__(
        self, *, comparison_weights: Iterable[ComparisonWeights], prior: float | None
    ):
        """Create a new Weights object."""
        self.comparison_weights = {cw.name: cw for cw in comparison_weights}
        self.prior = prior

    @classmethod
    def from_comparisons(
        cls, comparisons: Iterable[Comparison], prior: float | None = None
    ) -> Weights:
        comparison_weights = [ComparisonWeights.from_comparison(c) for c in comparisons]
        return cls(comparison_weights=comparison_weights, prior=prior)

    def __getitem__(self, name: str) -> ComparisonWeights:
        """Get a ComparisonWeights by name."""
        return self.comparison_weights[name]

    @property
    def is_trained(self) -> bool:
        """If all weights have been set."""
        return (
            all(cw.is_trained for cw in self.comparison_weights.values())
            and self.prior is not None
        )


class FellegiSunterComparer:
    """Compares two tables using the Fellegi-Sunter model.

    Contains a set of Comparisons. If trained, also contains a corresponding set
    of Weights. Otherwise, the weights are None.
    """

    comparisons: Comparisons
    """The Comparisons to use."""
    weights: Weights
    """The `prior` and set of `ComparisonWeights`, one for each `Comparison`."""

    def __init__(
        self,
        comparisons: Comparisons | Iterable[Comparison],
        weights: Weights | None = None,
    ):
        self.comparisons = Comparisons(*comparisons)
        if weights is None:
            weights = Weights.from_comparisons(comparisons)
        self.weights = weights

    def compare(self, blocked: Table) -> Table:
        """Compare record pairs, adding columns for each Comparison.

        For each Comparison, we add two columns:
        - {comparison.name}_cmp: the level of the comparison for each record pair.
          For example the column might be called "name_cmp" and have values like
          "exact_match", "phonetic_match", "no_match".
        - {comparison.name}_bf: the Bayes factor for each record pair.
          This is a number that describes how this comparison affects the likelihood
          of a match. For example, a Bayes factor of 10 means that this comparison
          increased the likelihood of a match by 10x as compared to if we hadn't
          looked at this comparison.
          For example, the column might be called "name_bf" and have values like
          10, 0.1, 1.

        In addition to these per-Comparison columns, we also add a column called "bf"
        which is the overall Bayes Factor for each record pair. We calculate this by
        starting with the prior probability of a match, and then multiplying by each
        Comparison's Bayes factor.
        """
        if not self.is_trained:
            raise ValueError(f"{self} is not trained.")

        total_bf = prob_to_bayes_factor(self.weights.prior)
        m = {}
        for comparison in self.comparisons:
            comparison_weights = self.weights[comparison.name]
            labels = comparison.label_pairs(blocked, how="index")
            bf = comparison_weights.bayes_factor(labels)
            m[f"{comparison.name}_cmp"] = comparison.label_pairs(blocked, how="name")
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
        """If all weights have been set."""
        return self.weights.is_trained

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.comparisons!r}, {self.weights!r})"
