from __future__ import annotations

import dataclasses
from typing import Callable, Iterable, Literal, overload

import ibis
from ibis.expr.types import BooleanValue, IntegerColumn, StringColumn, Table


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


class Comparison:
    """A measurement of how similar two records are.

    A Comparison is made up of multiple ComparisonLevels.
    We don't explicitly store an ELSE ComparisonLevel.
    If a record pair doesn't match any of the levels, it is considered
    to be an ELSE implicitly.

    This acts like an ordered, dict-like collection of ComparisonLevels.
    You can access the levels by index or by name, or iterate over them.
    """

    def __init__(
        self,
        name: str,
        levels: Iterable[ComparisonLevel],
        description: str | None = None,
    ):
        self._name = name
        self._levels = tuple(levels)
        self._description = description
        self._lookup: self._build_lookup(self._levels)

    @property
    def name(self) -> str:
        """The name of the comparison."""
        return self._name

    @property
    def levels(self) -> tuple[ComparisonLevel, ...]:
        """The levels of the comparison. Does not include the implicit ELSE level."""
        return self._levels

    @property
    def description(self) -> str | None:
        """A description of the comparison."""
        return self._description

    def __getitem__(self, name_or_index: str | int) -> ComparisonLevel:
        """Get a level by name or index."""
        return self._lookup[name_or_index]

    def __len__(self) -> int:
        """The number of levels. Does not include the implicit ELSE level."""
        return len(self.levels)

    @overload
    def label_pairs(self, pairs: Table, how: Literal["index"]) -> IntegerColumn:
        ...

    @overload
    def label_pairs(self, pairs: Table, how: Literal["name"]) -> StringColumn:
        ...

    def label_pairs(self, pairs, how="index"):
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it.
        This would be the ELSE case that is used in splink, but in our version
        we don't explicitly have an ELSE ComparisonLevel.

        Analogous to the gamma values described at
        https://www.robinlinacre.com/maths_of_fellegi_sunter/

        Parameters
        ----------
        pairs : Table
            A table of record pairs.
        how : {'index', 'name'}, default 'index'
            Whether to label the pairs with the index (uint8)
            or the name (string) of the level.

            If 'index', any ELSE results will be labelled as NULL.
            If 'name', any ELSE results will be labelled as 'else'.

        Returns
        """
        labels = ibis.NA
        for i, level in enumerate(self.levels):
            is_match = labels.isnull() & level.condition(pairs)
            label = ibis.literal(i, type="uint8") if how == "index" else level.name
            labels = is_match.ifelse(label, labels)
        if how == "name":
            labels = labels.fillna("else")
        return labels.name(self.name)

    def __repr__(self) -> str:
        levels_str = ", ".join(repr(level) for level in self.levels)
        if self.description is None:
            return f"Comparison(name={self.name}, levels=[{levels_str}])"
        else:
            return f"Comparison(name={self.name}, description={self.description}, levels=[{levels_str}])"  # noqa: E501

    @staticmethod
    def _build_lookup(
        levels: Iterable[ComparisonLevel],
    ) -> dict[str | int, ComparisonLevel]:
        lookup = {}
        for i, level in enumerate(levels):
            if level.name in lookup:
                raise ValueError(f"Duplicate level name: {level.name}")
            lookup[level.name] = level
            lookup[i] = level
        return lookup


class Comparisons:
    """An unordered, dict-like collection of `Comparison`s."""

    def __init__(self, comparisons: Iterable[Comparison] | Comparisons):
        if isinstance(comparisons, Comparisons):
            comparisons = list(comparisons)
        self._lookup: dict[str, Comparison] = {}
        for c in comparisons:
            if c.name in self._lookup:
                raise ValueError(f"Duplicate comparison name: {c.name}")
            if c.name == "else":
                raise ValueError(
                    "Comparison name 'else' is reserved for the ELSE case."
                )
            self._lookup[c.name] = c

    def label_pairs(
        self,
        blocked: Table,
        *,
        name_formatter: str | Callable[[str], str] = "",
        how: Literal["index", "name"] = "index",
    ) -> Table:
        m = {}
        for comparison in self:
            labels = comparison.label_pairs(blocked, how=how)
            name = _rename(name_formatter, comparison.name)
            m[name] = labels
        return blocked.mutate(**m)

    def __iter__(self) -> Iterable[Comparison]:
        return iter(self._lookup.values())

    def __getitem__(self, name: str) -> Comparison:
        return self._lookup[name]

    def __len__(self) -> int:
        return len(self._lookup)

    def __repr__(self) -> str:
        comps = ", ".join(repr(comp) for comp in self)
        return f"Comparisons({comps})"


def _rename(format: str | Callable[[str], str], s: str) -> str:
    if isinstance(format, str):
        if format == "":
            return s
        return format.format(name=s)
    else:
        return format(s)
