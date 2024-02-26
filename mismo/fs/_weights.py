from __future__ import annotations

from json import dumps, loads
import math
from pathlib import Path
from typing import Iterable, Iterator, overload

import altair as alt
from ibis.expr.types import FloatingValue, IntegerValue, StringValue, Table

from .._typing import Self
from ._util import odds_to_log_odds, odds_to_prob


class LevelWeights:
    """Weights for a single [AgreementLevel][mismo.compare.AgreementLevel].

    This describes for example "If zipcodes match perfectly, then
    this increases the probability of a match by 10x as compared to if we
    hadn't looked at zipcode".
    """

    def __init__(self, name: str, *, m: float, u: float) -> None:
        """Create a new LevelWeights object."""
        self._name = name
        self._m = m
        self._u = u

    @property
    def name(self) -> str:
        """The name of the level, e.g. "Exact Match"."""
        return self._name

    @property
    def m(self) -> float:
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

    def __eq__(self, other):
        if not isinstance(other, LevelWeights):
            return False
        return (
            self.name == other.name
            and math.isclose(self.m, other.m)
            and math.isclose(self.u, other.u)
        )


def _else_weights(other_level_weights: Iterable[LevelWeights]) -> LevelWeights:
    """A level that matches all record pairs that don't match any other level."""
    other_level_weights = list(other_level_weights)
    if any(w.name == "else" for w in other_level_weights):
        raise ValueError("Cannot have a level named 'else'")
    ms = [w.m for w in other_level_weights]
    us = [w.u for w in other_level_weights]
    else_m = 1 - sum(ms)
    else_u = 1 - sum(us)
    return LevelWeights(name="else", m=else_m, u=else_u)


class ComparisonWeights:
    """
    The weights for a single [LevelComparer][mismo.compare.LevelComparer].

    An ordered, dict-like collection of [LevelWeights][mismo.fs.LevelWeights]
    one for each level.
    """

    def __init__(self, name: str, level_weights: Iterable[LevelWeights]):
        """Create a new ComparisonWeights object."""
        self._name = name
        self._level_weights = tuple(level_weights) + (_else_weights(level_weights),)
        lookup = {}
        for i, lw in enumerate(self._level_weights):
            if lw.name in lookup:
                raise ValueError(f"Duplicate level name: {lw.name}")
            # 0 -> -len + 0
            # 1 -> -len + 1
            # 2 -> -len + 2
            negative_i = -len(self._level_weights) + i
            lookup[lw.name] = lw
            lookup[i] = lw
            lookup[negative_i] = lw
        self._lookup = lookup

    @property
    def name(self) -> str:
        """
        The name of the Comparison that these weights are for, eg "name" or "address".
        """
        return self._name

    @overload
    def __getitem__(self, name_or_index: str | int) -> LevelWeights:
        ...

    @overload
    def __getitem__(self, name_or_index: slice) -> tuple[LevelWeights, ...]:
        ...

    def __getitem__(
        self, name_or_index: str | int | slice
    ) -> LevelWeights | tuple[LevelWeights, ...]:
        """Get a LevelWeights by name or index."""
        if isinstance(name_or_index, slice):
            return self._level_weights[name_or_index]
        try:
            return self._lookup[name_or_index]
        except KeyError:
            raise KeyError(f"Unknown level name or index: {name_or_index}")

    def __contains__(self, name_or_index: str | int) -> bool:
        """Check if a LevelWeights is present by name or index."""
        return name_or_index in self._lookup

    def __iter__(self) -> Iterator[LevelWeights]:
        """Iterate over the LevelWeights, including the implicit ELSE level."""
        return iter(self._level_weights)

    def __len__(self) -> int:
        """The number of LevelWeights, including the implicit ELSE level."""
        return len(self._level_weights)

    def __repr__(self) -> str:
        return f"""{self.__class__.__name__}(
        name={self.name},
        level_weights={self._level_weights}
    )"""

    def __eq__(self, other):
        if not isinstance(other, ComparisonWeights):
            return False
        return self.name == other.name and self._level_weights == other._level_weights

    @overload
    def odds(self, labels: str | int) -> float:
        ...

    @overload
    def odds(self, labels: StringValue | IntegerValue) -> FloatingValue:
        ...

    def odds(
        self, labels: str | int | StringValue | IntegerValue
    ) -> float | FloatingValue:
        """Calculate the odds for each record pair.

        If `labels` is a string or integer, then we calculate the odds for that
        level. For example, if `labels` is "close", then we calculate the odds
        for the "close" level. If `labels` is 0, then we calculate the odds for
        the first level. If `labels` is -1, then we calculate the odds for the
        last level (the ELSE level).

        If `labels` is a StringValue or IntegerValue, then we do the same thing,
        except that we return an ibis FloatingValue instead of a python float.
        """
        if isinstance(labels, (int, str)):
            return self[labels].odds
        if isinstance(labels, StringValue):
            cases = [(lw.name, lw.odds) for lw in self]
        elif isinstance(labels, IntegerValue):
            cases = [(i, lw.odds) for i, lw in enumerate(self)]  # type: ignore # noqa: E501
        else:
            raise TypeError(
                f"Expected int, str, StringValue, or IntegerValue, got {type(labels)}"
            )
        return labels.cases(cases)

    @overload
    def match_probability(self, labels: str | int) -> float:
        ...

    @overload
    def match_probability(self, labels: StringValue | IntegerValue) -> FloatingValue:
        ...

    def match_probability(
        self, labels: str | int | StringValue | IntegerValue
    ) -> float | FloatingValue:
        """Calculate the match probability for each record pair."""
        return odds_to_prob(self.odds(labels))

    @overload
    def log_odds(self, labels: str | int) -> float:
        ...

    @overload
    def log_odds(self, labels: StringValue | IntegerValue) -> FloatingValue:
        ...

    def log_odds(
        self, labels: str | int | StringValue | IntegerValue
    ) -> float | FloatingValue:
        """Calculate the log odds for each record pair."""
        return odds_to_log_odds(self.odds(labels))

    @staticmethod
    def plot(self) -> alt.Chart:
        """Plot the weights for this comparison."""
        from ._plot import plot_weights

        return plot_weights(self)


class Weights:
    """Weights for the Fellegi-Sunter model.

    An unordered, dict-like collection of
    [ComparisonWeights][mismo.fs.ComparisonWeights],
    one for each [LevelComparer][mismo.compare.LevelComparer] of the same name.
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

    def __len__(self) -> int:
        """The number of `ComparisonWeights`."""
        return len(self._lookup)

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
        naming = {}
        for comparison_weights in self:
            name = comparison_weights.name
            labels = compared[name]
            odds = comparison_weights.odds(labels)
            m[f"{name}_odds"] = odds
            naming[f"{name}_odds"] = name
            total_odds *= odds
        m["odds"] = total_odds
        result = compared.mutate(**m)
        # Don't do any of this relocation in terms of record_id, etc
        # because the passed in table doesn't need ot have these.
        # It only needs to have the labels for each comparison.
        for odds_name, name in naming.items():
            result = result.relocate(odds_name, after=name)
        result = result.relocate("odds", before=list(naming.values())[0])
        return result

    def plot(self) -> alt.Chart:
        """Plot the weights for all of the Comparisons."""
        from ._plot import plot_weights

        return plot_weights(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}{tuple(self)}"

    def __eq__(self, other):
        if not isinstance(other, Weights):
            return False
        a = sorted(self, key=lambda cw: cw.name)
        b = sorted(other, key=lambda cw: cw.name)
        return a == b

    def to_json(self, path: str | Path | None = None) -> dict:
        """Return a JSON-serializable representation of the weights.

        If `path` is given, write the dict to the file at that path in addition
        to returning it.
        """
        d = {
            cw.name: {lw.name: {"m": lw.m, "u": lw.u} for lw in cw if lw.name != "else"}
            for cw in self
        }
        if path is not None:
            Path(path).write_text(dumps(d))
        return d

    @classmethod
    def from_json(cls, json: dict | str | Path) -> Self:
        """Create a Weights object from a JSON-serializable representation.

        Parameters
        ----------
        json : dict | str | Path
            If a dict, assumed to be the JSON-serializable representation.
            Load it directly.
            If a str or Path, assumed to be a path to a JSON file.
            Load it from that file.

        Returns
        -------
        Weights
            The Weights object created from the JSON-serializable representation.
        """
        if not isinstance(json, dict):
            json = loads(Path(json).read_text())
        return cls(
            ComparisonWeights(
                name=comparison_name,
                level_weights=[
                    LevelWeights(name=level_name, m=level["m"], u=level["u"])
                    for level_name, level in comparison.items()
                ],
            )
            for comparison_name, comparison in json.items()
        )
