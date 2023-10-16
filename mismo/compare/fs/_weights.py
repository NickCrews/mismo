from __future__ import annotations

import dataclasses
from typing import Iterable

from ibis.expr.types import FloatingValue, IntegerValue, StringValue, Table

from mismo.compare._comparison import Comparison

from ._util import odds_to_prob


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
    def odds(self) -> float:
        """How much more likely is a match than a non-match at this level?

        This is derived from m and u. This is the same thing as "Bayes Factor"
        in splink.

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

    def odds(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the odds for each record pair."""
        if not self.is_trained:
            raise ValueError("Weights have not been set for all comparison levels.")
        if isinstance(labels, StringValue):
            cases = [(lw.name, lw.odds) for lw in self.level_weights]
        else:
            cases = [(i, lw.odds) for i, lw in enumerate(self.level_weights)]  # type: ignore # noqa: E501
        return labels.cases(cases, self.else_weights.odds)  # type: ignore

    def match_probability(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the match probability for each record pair."""
        return odds_to_prob(self.odds(labels))

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

    An unordered, dict-like collection of `ComparisonWeights`, one for each Comparison
    of the same name.
    """

    def __init__(self, comparison_weights: Iterable[ComparisonWeights]):
        """Create a new Weights object."""
        self._lookup = {cw.name: cw for cw in comparison_weights}

    def __getitem__(self, name: str) -> ComparisonWeights:
        """Get a `ComparisonWeights` by name."""
        return self._lookup[name]

    def __iter__(self) -> Iterable[ComparisonWeights]:
        """Iterate over the contained `ComparisonWeights`."""
        return iter(self._lookup.values())

    @property
    def is_trained(self) -> bool:
        """If all weights have been set."""
        return all(cw.is_trained for cw in self)

    def score(self, compared: Table) -> Table:
        """Score each already-compared record pair.

        For each Comparison, we add a column, `{comparison.name}_odds`.
        This is a number that describes how this comparison affects the likelihood
        of a match. For example, an odds of 10 means that this comparison
        increased the likelihood of a match by 10x as compared to if we hadn't
        looked at this comparison.
        For example, the column might be called "name_odds" and have values like
        10, 0.1, 1.

        In addition to these per-Comparison columns, we also add a column called "odds"
        which is the overall odds for each record pair. We calculate this by
        starting with the odds of 1 and then multiplying by each Comparison's odds
        to get the overall odds.
        """
        if not self.is_trained:
            raise ValueError(f"{self} is not trained.")

        total_odds = 1
        m = {}
        for comparison_weights in self:
            name = comparison_weights.name
            labels = compared[name]
            odds = comparison_weights.odds(labels)
            m[f"{name}_odds"] = odds
            total_odds *= odds
        m["odds"] = total_odds
        return compared.mutate(**m)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{tuple(self)}"
