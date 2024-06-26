from __future__ import annotations

from abc import ABCMeta
from typing import Iterable, Iterator, Literal, Type, TypeVar, overload

import ibis
from ibis.expr import types as ir

from mismo import _typing, _util


class _LevelsMeta(ABCMeta):
    def __new__(cls, name, bases, dct):
        s2i = {
            k: v for k, v in dct.items() if not k.startswith("_") and isinstance(v, int)
        }
        val_to_names = {v: set() for v in s2i.values()}
        for k, v in s2i.items():
            val_to_names[v].add(k)
        dupes = {}
        for v, names in val_to_names.items():
            if len(names) > 1:
                for n in names:
                    dupes[n] = v
        if dupes:
            raise TypeError(f"Duplicate values: {dupes}")
        i2s = {i: s for s, i in s2i.items()}

        newcls = super().__new__(cls, name, bases, dct)
        newcls.__s2i__ = s2i
        newcls.__i2s__ = i2s
        newcls.__annotations__ = {
            **_typing.get_annotations(newcls),
            **{k: newcls for k in s2i},
        }
        for s in s2i:
            setattr(newcls, s, newcls(s))
        return newcls

    @overload
    def __getitem__(self, key: str) -> int: ...

    @overload
    def __getitem__(self, key: int) -> str: ...

    def __getitem__(self, key):
        if isinstance(key, str):
            return self.__s2i__[key]
        elif isinstance(key, int):
            return self.__i2s__[key]
        else:
            raise TypeError(f"Invalid key: {key}")

    def __contains__(self, key: str | int) -> bool:
        return key in self.__s2i__ or key in self.__i2s__

    def __iter__(self) -> Iterator[str]:
        return iter(self.__s2i__.keys())

    def __len__(self) -> int:
        return len(self.__s2i__)

    def __repr__(self) -> str:
        pairs = (f"{k}={v}" for k, v in self.__s2i__.items())
        return f"{self.__name__}({', '.join(pairs)})"


class MatchLevel(metaclass=_LevelsMeta):
    """An enum-like class for match levels.

    This class is used to define the levels of agreement between two records.

    Examples
    --------
    >>> from mismo.compare import MatchLevel
    >>> class NameMatchLevel(MatchLevel):
    ...     EXACT = 0
    ...     NEAR = 1
    ...     ELSE = 2
    ...
    >>> NameMatchLevel.EXACT
    EXACT
    >>> NameMatchLevel.EXACT.as_string()
    'EXACT'
    >>> NameMatchLevel.EXACT.as_integer()
    0
    >>> len(NameMatchLevel)
    3
    >>> 2 in NameMatchLevel
    True
    >>> NameMatchLevel[1]
    'NEAR'

    You can construct your own values:

    >>> NameMatchLevel("NEAR").as_integer()
    1
    >>> NameMatchLevel(2).as_string()
    'ELSE'
    >>> NameMatchLevel(3) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Invalid value: 3. Must be one of {0, 1, 2}

    The powerful thing is it can be used to convert between string and integer
    ibis expressions:

    >>> import ibis
    >>> levels_raw = ibis.array([0, 2, 1, 99]).unnest()
    >>> levels = NameMatchLevel(levels_raw)
    >>> levels.as_string().execute()
    0    EXACT
    1     ELSE
    2     NEAR
    3     None
    Name: NameMatchLevel, dtype: object
    >>> levels.as_integer().execute()
    0     0
    1     2
    2     1
    3    99
    Name: Array(), dtype: int8
    """

    def __init__(
        self, value: MatchLevel | int | str | ir.StringValue | ir.IntegerValue
    ):
        """Create a new match level value.

        If the given value is a python int or str, it is checked against the
        valid values for this class. If it is an ibis expression,
        we do no such check.

        Parameters
        ----------
        value :
            The value of the match level.
        """
        if not isinstance(
            value, (MatchLevel, ir.StringValue, ir.IntegerValue, int, str)
        ):
            raise TypeError(f"Invalid value: {value}")
        valid_ints = set(self.__i2s__.keys())
        valid_strs = set(self.__s2i__.keys())
        if isinstance(value, int) and value not in valid_ints:
            raise ValueError(f"Invalid value: {value}. Must be one of {valid_ints}")
        if isinstance(value, str) and value not in valid_strs:
            raise ValueError(f"Invalid value: {value}. Must be one of {valid_strs}")
        if isinstance(value, MatchLevel):
            self._value = value._value
        else:
            self._value = value

    def as_integer(self) -> int | ir.IntegerValue:
        """Convert to a python int or ibis integer, depending on the original type."""
        v = self._value
        if isinstance(v, (int, ir.IntegerValue)):
            return v
        elif isinstance(v, str):
            return self.__s2i__.get(v, None)
        elif isinstance(v, ir.StringValue):
            return v.substitute(self.__s2i__, else_=ibis.null()).name(
                self.__class__.__name__
            )
        else:
            raise TypeError(f"Invalid value: {v}")

    def as_string(self) -> str | ir.StringValue:
        """Convert to a python str or ibis string, depending on the original type."""
        v = self._value
        if isinstance(v, (str, ir.StringValue)):
            return v
        elif isinstance(v, int):
            return self.__i2s__.get(v, None)
        elif isinstance(v, ir.IntegerValue):
            return v.substitute(self.__i2s__, else_=ibis.null()).name(
                self.__class__.__name__
            )
        else:
            raise TypeError(f"Invalid value: {v}")

    def __repr__(self) -> str:
        return self.as_string()

    def __eq__(self, other):
        if isinstance(other, MatchLevel):
            return self.as_integer() == other.as_integer()
        elif isinstance(other, int):
            return self.as_integer() == other
        elif isinstance(other, str):
            return self.as_string() == other
        elif isinstance(other, ir.NumericValue):
            return self.as_integer() == other
        elif isinstance(other, ir.StringValue):
            return self.as_string() == other
        else:
            return NotImplemented


MatchLevelT = TypeVar("MatchLevelT", bound=MatchLevel)


class LevelComparer:
    """
    Assigns a MatchLevel to record pairs based on one dimension, e.g. *name*
    """

    def __init__(
        self,
        name: str,
        levels: Type[MatchLevelT],
        cases: Iterable[tuple[ir.BooleanColumn, MatchLevelT]],
        *,
        representation: Literal["string", "integer"] = "integer",
    ):
        self.name = name
        self.levels = levels
        self.cases = tuple((c, self.levels(lev)) for c, lev in cases)
        self.representation = representation

    name: str
    """The name of the comparer, eg "date", "address", "latlon", "price"."""
    levels: Type[MatchLevelT]
    """The levels of agreement."""
    cases: tuple[tuple[ir.BooleanColumn, MatchLevelT], ...]
    """The cases to check for each level."""
    representation: Literal["string", "integer"] = "integer"
    """The native representation of the levels in ibis expressions.

    Integers are more performant, but strings are more human-readable.
    """

    def __call__(
        self,
        pairs: ir.Table,
        *,
        representation: Literal["string", "integer"] | None = None,
    ) -> ir.StringColumn | ir.IntegerColumn:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label ir.
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
        if representation is None:
            representation = self.representation

        cases = [(pairs.bind(c)[0], level) for c, level in self.cases]
        if representation == "string":
            cases = [(c, level.as_string()) for c, level in cases]
        elif representation == "integer":
            cases = [(c, level.as_integer()) for c, level in cases]
        else:
            raise ValueError(f"Invalid representation: {representation}")

        return pairs.mutate(_util.cases(*cases).name(self.name))

    def __repr__(self) -> str:
        return f"LevelComparer(name={self.name}, levels=[{self.levels}])"
