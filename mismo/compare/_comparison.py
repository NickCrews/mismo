from __future__ import annotations

import dataclasses
from typing import Callable, Literal

import ibis
from ibis.expr.types import BooleanValue, IntegerColumn, Table


@dataclasses.dataclass(frozen=True)
class ComparisonLevel:
    """A Level within a Comparison."""

    name: str
    condition: Callable[[Table], BooleanValue]
    description: str | None = None

    def __repr__(self) -> str:
        if self.description is None:
            return f"ComparisonLevel(name={self.name})"
        else:
            return f"ComparisonLevel(name={self.name}, description={self.description})"


@dataclasses.dataclass(frozen=True)
class Comparison:
    """A measurement of how similar two records are.

    A Comparison is made up of multiple ComparisonLevels.
    """

    name: str
    levels: list[ComparisonLevel]
    description: str | None = None

    def label_pairs(
        self, pairs: Table, label: Literal["index"] | Literal["name"] = "index"
    ) -> IntegerColumn:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it
        with the level's index. If it doesn't match any level, it has the label NA.
        This would be the ELSE case that is used in splink, but in our version
        we don't explicitly have an ELSE ComparisonLevel.

        Equivalent to the gamma values described at
        https://www.robinlinacre.com/maths_of_fellegi_sunter/"""
        labels = ibis.NA
        for i, level in enumerate(self.levels):
            is_match = labels.isnull() & level.condition(pairs)
            lab = i if label == "index" else level.name
            labels = is_match.ifelse(lab, labels)
        if label == "name":
            labels = labels.fillna("else")
        return labels.name(self.name)

    def __repr__(self) -> str:
        if self.description is None:
            return f"Comparison(name={self.name}, levels={self.levels})"
        else:
            return f"Comparison(name={self.name}, description={self.description}, levels={self.levels})"  # noqa: E501
