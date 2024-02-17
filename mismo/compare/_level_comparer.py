from __future__ import annotations

from typing import Callable, Iterable, Iterator, TypedDict, overload

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo import _util


class AgreementLevel(TypedDict):
    """A level of agreement such as *exact*, *phonetic*, or *within_1_day*.

    A AgreementLevel is a named condition that determines whether a record pair
    matches that level.
    """  # noqa: E501

    name: str
    """The name of the level. Should be short and unique within a LevelComparer.

    Examples:

    - "exact"
    - "misspelling"
    - "phonetic"
    - "within_1_day"
    - "within_1_km"
    - "within_10_percent"
    """
    condition: bool | Deferred | Callable[[it.Table], it.BooleanValue]
    """
    A condition that determines whether a record pair matches this level.

    Note that if this AgreementLevel is used in conjunction with other
    AgreementLevels, the condition is implcitly dependent on the previous
    levels. For example, if the previous level's condition is
    `_.name_l.upper() == _.name_r.upper()`, then if this level's condition is
    `_.name_l == _.name_r`, this level is useless, and will match no records pairs,
    because they all would have been matched by the previous level.

    Examples:

    - `_.name_l == _.name_r`
    - `lambda t: (t.cost_l - t.cost_r).abs() / t.cost_l < 0.1`
    - `True`
    """


_ELSE_LEVEL = AgreementLevel(
    name="else",
    condition=True,
)


class LevelComparer:
    """
    Assigns a level of similarity to record pairs based on one dimension, e.g. *name*

    This acts like an ordered, dict-like collection of
    [AgreementLevels][mismo.compare.AgreementLevel].
    You can access the levels by index or by name, or iterate over them.
    The last level is always an `else` level, which matches all record pairs
    if none of the previous levels matched.
    """

    def __init__(
        self,
        name: str,
        levels: Iterable[AgreementLevel],
    ):
        """Create a LevelComparer.

        Parameters
        ----------
        name :
            The name of the comparer, eg "date", "address", "latlon", "price".
        levels :
            The levels of agreement. You may include an `else` level as a final
            level that matches everything, or it will be added automatically if
            you don't include one.
        """
        self._name = name
        self._levels, self._lookup = self._parse_levels(levels)

    @property
    def name(self) -> str:
        """The name of the comparer, eg "date", "address", "latlon", "price"."""
        return self._name

    @overload
    def __getitem__(self, name_or_index: str | int) -> AgreementLevel:
        ...

    @overload
    def __getitem__(self, name_or_index: slice) -> tuple[AgreementLevel, ...]:
        ...

    def __getitem__(self, name_or_index):
        """Get a level by name or index."""
        if isinstance(name_or_index, (int, slice)):
            return self._levels[name_or_index]
        return self._lookup[name_or_index]

    def __iter__(self) -> Iterator[AgreementLevel]:
        """Iterate over the levels, including the ELSE level."""
        return iter(self._levels)

    def __len__(self) -> int:
        """The number of levels, including the ELSE level."""
        return len(self._levels)

    def __call__(self, pairs: it.Table) -> it.StringColumn:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label it.
        If none of the levels match a pair, it labeled as "else".

        Parameters
        ----------
        pairs : Table
            A table of record pairs.
        Returns
        -------
        labels : StringColumn
            The labels for each record pair.
        """
        labels = ibis.case()
        # Skip the ELSE level, do that ourselves. This is to avoid if someone
        # mis-specifies the ELSE level condition so that it doesn't
        # match everything.
        for level in self[:-1]:
            labels = labels.when(
                _util.get_column(pairs, level["condition"]), level["name"]
            )
        labels = labels.else_("else")
        return labels.end().name(self.name)

    def __repr__(self) -> str:
        levels_str = ", ".join(repr(level) for level in self)
        return f"{self.__class__.__name__}(name={self.name}, levels=[{levels_str}])"

    @classmethod
    def _parse_levels(
        cls,
        levels: Iterable[AgreementLevel],
    ) -> tuple[tuple[AgreementLevel], dict[str | int, AgreementLevel]]:
        levels = tuple(levels)
        rest, last = levels[:-1], levels[-1]
        for level in rest:
            if level["name"] == "else":
                raise ValueError(
                    f"ELSE AgreementLevel must be the last level in a {cls.__name__}."
                )
        if last["name"] != "else":
            levels = (*levels, _ELSE_LEVEL)

        lookup = {}
        for i, level in enumerate(levels):
            name = level["name"]
            if name in lookup:
                raise ValueError(f"Duplicate level name: {name}")
            lookup[name] = level
            lookup[i] = level
        return levels, lookup
