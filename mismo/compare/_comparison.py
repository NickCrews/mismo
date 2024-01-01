from __future__ import annotations

import dataclasses
from typing import Callable, Iterable, Iterator, Literal, overload

import ibis
from ibis.common.deferred import Deferred
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
    condition: bool | Deferred | Callable[[Table], BooleanValue]
    """
    A condition that determines whether a record pair matches this level.

    Examples:

    - `_.name_l == _.name_r`
    - `lambda t: (t.cost_l - t.cost_r).abs() / t.cost_l < 0.1`
    - `True`
    """

    def is_match(self, pairs: Table) -> BooleanValue:
        """Determine whether a record pair matches this level.

        Uses `self.condition` to determine whether a record pair matches this level.

        Parameters
        ----------
        pairs : Table
            A table of record pairs.
        """
        if isinstance(self.condition, bool):
            return ibis.literal(self.condition)
        elif isinstance(self.condition, Deferred):
            return self.condition.resolve(pairs)
        else:
            return self.condition(pairs)

    def __repr__(self) -> str:
        return f"ComparisonLevel(name={self.name})"


_ELSE_LEVEL = ComparisonLevel(
    name="else",
    condition=True,
)


class Comparison:
    """
    A measure of record pair similarity based on one dimension, e.g. *name* or *date*.

    This acts like an ordered, dict-like collection of
    [ComparisonLevels](#mismo.block.ComparisonLevels).
    You can access the levels by index or by name, or iterate over them.
    The last level is always an `else` level, which matches all record pairs
    if none of the previous levels matched.
    """

    def __init__(
        self,
        name: str,
        levels: Iterable[ComparisonLevel],
    ):
        """Create a Comparison.

        Parameters
        ----------
        name : str
            The name of the comparison. Must be unique within a set of Comparisons.
        levels : Iterable[ComparisonLevel]
            The levels of the comparison. You may include an `else` level as a final
            level that matches everything, or it will be added automatically if
            you don't include one.
        """
        self._name = name
        self._levels, self._lookup = self._parse_levels(levels)

    @property
    def name(self) -> str:
        """The name of the comparison. Must be unique within a set of Comparisons.

        An example might be "name", "date", "address", "latlon", "price".
        """
        return self._name

    @overload
    def __getitem__(self, name_or_index: str | int) -> ComparisonLevel:
        ...

    @overload
    def __getitem__(self, name_or_index: slice) -> tuple[ComparisonLevel]:
        ...

    def __getitem__(self, name_or_index):
        """Get a level by name or index."""
        if isinstance(name_or_index, (int, slice)):
            return self._levels[name_or_index]
        return self._lookup[name_or_index]

    def __iter__(self) -> Iterator[ComparisonLevel]:
        """Iterate over the levels, including the ELSE level."""
        return iter(self._levels)

    def __len__(self) -> int:
        """The number of levels, including the ELSE level."""
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
        labels = ibis.case()
        for i, level in enumerate(self[:-1]):
            label = ibis.literal(i, type="uint8") if how == "index" else level.name
            labels = labels.when(level.is_match(pairs), label)
        if how == "name":
            labels = labels.else_("else")
        else:
            labels = labels.else_(None)
        return labels.end().name(self.name)

    def __repr__(self) -> str:
        levels_str = ", ".join(repr(level) for level in self)
        return f"Comparison(name={self.name}, levels=[{levels_str}])"

    @staticmethod
    def _parse_levels(
        levels: Iterable[ComparisonLevel],
    ) -> tuple[tuple[ComparisonLevel], dict[str | int, ComparisonLevel]]:
        levels = tuple(levels)
        rest, last = levels[:-1], levels[-1]
        for level in rest:
            if level.name == "else":
                raise ValueError(
                    "ELSE ComparisonLevel must be the last level in a Comparison."
                )
        if last.name != "else":
            levels = (*levels, _ELSE_LEVEL)

        lookup = {}
        for i, level in enumerate(levels):
            if level.name in lookup:
                raise ValueError(f"Duplicate level name: {level.name}")
            lookup[level.name] = level
            lookup[i] = level
        return levels, lookup


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
        if "record_id_r" in result.columns:
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
