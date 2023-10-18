from __future__ import annotations

import dataclasses
from typing import Callable, Iterable, Literal, overload, Iterator

import ibis
from ibis.expr.types import BooleanValue, IntegerColumn, StringColumn, Table


@dataclasses.dataclass(frozen=True)
class ComparisonLevel:
    """A Level within a [Comparison](#mismo.block.Comparison), such as *exact*, *phonetic*, or *within_1_day*.

    A ComparisonLevel is a named condition that determines whether a record pair
    matches that level.
    """  # noqa: E501

    name: str
    """The name of the level. Should be short and unique within a Comparison.

    Examples:

    - "exact"
    - "misspelling"
    - "phonetic"
    - "within_1_day"
    - "within_1_km"
    - "within_10_percent"
    """
    condition: Callable[[Table], BooleanValue]
    """
    A condition that determines whether a record pair matches this level.

    Examples:

    - `lambda t: t.name_l == t.name_r`
    - `lambda t: (t.cost_l - t.cost_r).abs() / t.cost_l < 0.1`
    """
    description: str | None = None
    """A description of the level. Intended for humans in charts and documentation.

    Not needed for functionality.
    """

    def __repr__(self) -> str:
        if self.description is None:
            return f"ComparisonLevel(name={self.name})"
        else:
            return f"ComparisonLevel(name={self.name}, description={self.description})"


_ELSE_LEVEL = ComparisonLevel(
    name="else",
    condition=lambda _: True,
    description="None of the previous levels match.",
)


class Comparison:
    """
    A measure of record pair similarity based on one dimension, e.g. *name* or *date*.

    This acts like an ordered, dict-like collection of
    [ComparisonLevels](#mismo.block.ComparisonLevels).
    You can access the levels by index or by name, or iterate over them.
    We don't explicitly store an `ELSE` ComparisonLevel.
    If a record pair doesn't match any of the level conditions, it is considered
    to be an `ELSE` implicitly.
    """

    def __init__(
        self,
        name: str,
        levels: Iterable[ComparisonLevel],
        description: str | None = None,
    ):
        """Create a Comparison.

        Parameters
        ----------
        name : str
            The name of the comparison. Must be unique within a set of Comparisons.
        levels : Iterable[ComparisonLevel]
            The levels of the comparison. Do not include the implicit `ELSE` level.
        description : str, optional
            A description of the comparison. Intended for humans and documentation.
            Not needed for functionality.
        """
        self._name = name
        self._levels = tuple(levels) + (_ELSE_LEVEL,)
        self._description = description
        self._lookup = self._build_lookup(self._levels)

    @property
    def name(self) -> str:
        """The name of the comparison. Must be unique within a set of Comparisons.

        An example might be "name", "date", "address", "latlon", "price".
        """
        return self._name

    @property
    def description(self) -> str | None:
        """A description of the comparison. This is optional and intended for humans."""
        return self._description

    def __getitem__(self, name_or_index: str | int | slice) -> ComparisonLevel:
        """Get a level by name or index."""
        if isinstance(name_or_index, (int, slice)):
            return self._levels[name_or_index]
        return self._lookup[name_or_index]

    def __iter__(self) -> Iterator[ComparisonLevel]:
        """Iterate over the levels, including the implicit ELSE level."""
        return iter(self._levels)

    def __len__(self) -> int:
        """The number of levels, including the implicit ELSE level."""
        return len(self._levels)

    @overload
    def label_pairs(
        self, pairs: Table, how: Literal["index"] = "index"
    ) -> IntegerColumn:
        ...

    @overload
    def label_pairs(self, pairs: Table, how: Literal["name"] = "index") -> StringColumn:
        ...

    def label_pairs(self, pairs, how="index"):
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it.

        If none of the levels match a pair, it labeled as an ELSE.

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
        -------
        labels : IntegerColumn | StringColumn
            The labels for each record pair.
        """
        labels = ibis.NA
        for i, level in enumerate(self):
            is_match = labels.isnull() & level.condition(pairs)
            label = ibis.literal(i, type="uint8") if how == "index" else level.name
            labels = is_match.ifelse(label, labels)
        if how == "name":
            labels = labels.fillna("else")
        return labels.name(self.name)

    def __repr__(self) -> str:
        levels_str = ", ".join(repr(level) for level in self)
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
    """An unordered, dict-like collection of [Comparison](#mismo.block.Comparison)s."""

    def __init__(self, *comparisons: Comparison):
        """Create a set of Comparisons.

        Parameters
        ----------
        comparisons : Iterable[Comparison] | Comparisons
            The comparisons to include in the set.
        """
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
        how: Literal["index", "name"] = "index",
    ) -> Table:
        """Label each record pair for each Comparison.

        Adds columns to the blocked table, one for each Comparison.
        The columns are named after the Comparison, and contain the label for each
        record pair.

        Parameters
        ----------
        blocked : Table
            A table of blocked record pairs.
        how : {'index', 'name'}, default 'index'
            Whether to label the pairs with the index (uint8)
            or the name (string) of the level.

            If 'index', any ELSE results will be labelled as NULL.
            If 'name', any ELSE results will be labelled as 'else'.
        """
        m = {}
        for comparison in self:
            labels = comparison.label_pairs(blocked, how=how)
            m[comparison.name] = labels
        result = blocked.mutate(**m)
        result = result.relocate(*m.keys(), after="record_id_r")
        return result

    def __iter__(self) -> Iterator[Comparison]:
        """Iterate over the comparisons."""
        return iter(self._lookup.values())

    def __getitem__(self, name: str) -> Comparison:
        """Get a comparison by name."""
        return self._lookup[name]

    def __len__(self) -> int:
        """The number of comparisons."""
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
