from __future__ import annotations

from typing import Iterable, Iterator

import altair as alt
from ibis.expr.types import FloatingValue, IntegerValue, StringValue, Table

from ._util import odds_to_log_odds, odds_to_prob


class LevelWeights:
    """Weights for a single level of a Comparison.

    This describes for example "If zipcodes match perfectly, then
    this increases the probability of a match by 10x as compared to if we
    hadn't looked at zipcode".
    """

    def __init__(self, name: str, *, m: float, u: float):
        """Create a new LevelWeights object."""
        self._name = name
        self._m = m
        self._u = u

    @property
    def name(self) -> str:
        """The name of the level, e.g. "Exact Match"."""
        return self._name

    @property
    def m(self) -> float | None:
        """Among true-matches, what proportion of them have this level?

        1 means this level is a good indication of a match, 0 means it's a good
        indication of a non-match.
        """
        return self._m

    @property
    def u(self) -> float:
        """Among non-matches, what proportion of them have this level?

        1 means this level is a good indication of a non-match, 0 means it's a good
        indication of a match.
        """
        return self._u

    @property
    def odds(self) -> float:
        """How much more likely is a match than a non-match at this level?

        This is derived from m and u. This is the same thing as "Bayes Factor"
        in splink.

        - values below 1 is evidence against a match
        - values above 1 is evidence for a match
        - 1 means this level does not provide any evidence for or against a match
        """
        if self.u == 0:
            return float("inf")
        else:
            return self.m / self.u

    @property
    def log_odds(self) -> float:
        """The log base 10 of the odds."""
        return odds_to_log_odds(self.odds)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, m={self.m}, u={self.u})"


class ComparisonWeights:
    def __init__(self, name: str, level_weights: Iterable[LevelWeights]):
        """Create a new ComparisonWeights object."""
        self._name = name
        self._level_weights = tuple(level_weights)
        lookup = {}
        for i, lw in enumerate(self._level_weights):
            if lw.name in lookup:
                raise ValueError(f"Duplicate level name: {lw.name}")
            lookup[lw.name] = lw
            lookup[i] = lw
        self._lookup = lookup

    @property
    def name(self) -> str:
        """The name of the Comparison that these weights are for, eg "name" or "address"."""
        return self._name

    @property
    def level_weights(self) -> list[LevelWeights]:
        """The weights for each level of the Comparison."""
        return list(self._lookup.values())

    def __getitem__(self, name_or_index: str | int) -> LevelWeights:
        """Get a LevelWeights by name or index."""
        return self._lookup[name_or_index]

    def __iter__(self) -> Iterator[LevelWeights]:
        """Iterate over the LevelWeights."""
        return iter(self._level_weights)

    def __len__(self) -> int:
        """The number of LevelWeights. Does not include the implicit ELSE level."""
        return len(self._level_weights)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, level_weights={self._level_weights})"

    def odds(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the odds for each record pair."""
        if isinstance(labels, StringValue):
            cases = [(lw.name, lw.odds) for lw in self]
        else:
            cases = [(i, lw.odds) for i, lw in enumerate(self)]  # type: ignore # noqa: E501
        return labels.cases(cases, self.else_weights.odds)  # type: ignore

    def match_probability(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the match probability for each record pair."""
        return odds_to_prob(self.odds(labels))

    def log_odds(self, labels: IntegerValue | StringValue) -> FloatingValue:
        """Calculate the log odds for each record pair."""
        return odds_to_log_odds(self.odds(labels))

    @property
    def else_weights(self) -> LevelWeights:
        """A level that matches all record pairs that don't match any other level."""
        ms = [w.m for w in self]  # type: ignore
        us = [w.u for w in self]  # type: ignore
        else_m = 1 - sum(ms)
        else_u = 1 - sum(us)
        return LevelWeights(name="else", m=else_m, u=else_u)

    def plot(self) -> alt.Chart:
        """Plot the weights for this comparison."""
        from ._plot import plot_weights

        return plot_weights(self)


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

    def __iter__(self) -> Iterator[ComparisonWeights]:
        """Iterate over the contained `ComparisonWeights`."""
        return iter(self._lookup.values())

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
        total_odds = 1
        m = {}
        for comparison_weights in self:
            name = comparison_weights.name
            labels = compared[name]
            odds = comparison_weights.odds(labels)
            m[f"{name}_odds"] = odds
            total_odds *= odds
        m["odds"] = total_odds
        result = compared.mutate(**m)
        for cw in self:
            result = result.relocate(f"{cw.name}_odds", after=cw.name)
        result = result.relocate("odds", after="record_id_r")
        return result

    def plot(self) -> alt.Chart:
        """Plot the weights for all of the Comparisons."""
        from ._plot import plot_weights

        return plot_weights(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{tuple(self)}"
