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

    @overload
    def __getitem__(self, key: ir.NumericValue) -> ir.StringValue: ...

    @overload
    @classmethod
    def __getitem__(self, key: ir.StringValue) -> ir.IntegerValue: ...

    def __getitem__(self, key):
        """Converts string-likes to int-likes, and vice-versa.

        Ibis expressions are converted, but if you supply a bad value like
        "bogus level name", we can't type-check you at call time,
        and down the line when you actually .execute() the results you will get an
        ibis NULL value.

        Examples
        --------
        >>> from mismo.compare import MatchLevel
        >>> class NameMatchLevel(MatchLevel):
        ...     EXACT = 0
        ...     NEAR = 1
        ...     ELSE = 2

        >>> NameMatchLevel[1]
        'NEAR'
        >>> NameMatchLevel["NEAR"]
        1
        >>> NameMatchLevel[ibis.literal(1)].execute()
        'NEAR'
        >>> NameMatchLevel[ibis.literal("NEAR")].execute()
        1
        >>> NameMatchLevel[100]
        Traceback (most recent call last):
            ...
        KeyError: Invalid value: 100. Must be one of {0, 1, 2}
        >>> NameMatchLevel[ibis.literal(100)].execute() is None
        True
        """
        if _is_stringy(key):
            return self._str_to_int(key)
        elif _is_inty(key):
            return self._int_to_str(key)
        else:
            raise TypeError(f"Invalid key: {key}")

    def __contains__(self, key: str | int) -> bool:
        """Check if an int or str is one of the levels."""
        if not isinstance(key, (str, int)):
            raise TypeError(f"can only check containment for ints and strs, got {key}")
        return key in self.__s2i__ or key in self.__i2s__

    def __iter__(self) -> Iterator[str]:
        """The names of the levels."""
        return iter(self.__s2i__.keys())

    def __len__(self) -> int:
        """The number of levels"""
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

    The class acts as a container:

    >>> len(NameMatchLevel)
    3
    >>> 2 in NameMatchLevel
    True
    >>> list(NameMatchLevel)
    ['EXACT', 'NEAR', 'ELSE']

    You can access the hardcoded values:

    >>> str(NameMatchLevel.EXACT)
    'EXACT'
    >>> int(NameMatchLevel.EXACT)
    0

    You can use indexing semantics to translate between strings and ints:

    >>> NameMatchLevel[1]
    'NEAR'
    >>> NameMatchLevel["NEAR"]
    1
    >>> NameMatchLevel[ibis.literal(1)].execute()
    'NEAR'
    >>> NameMatchLevel[ibis.literal("NEAR")].execute()
    1

    You can construct your own values, both from python literals...

    >>> NameMatchLevel("NEAR").as_integer()
    1
    >>> NameMatchLevel(2).as_string()
    'ELSE'
    >>> NameMatchLevel(3)
    Traceback (most recent call last):
    ...
    ValueError: Invalid value: 3. Must be one of {0, 1, 2}`

    ...And Ibis expressions

    >>> import ibis
    >>> levels_raw = ibis.array([0, 2, 1, 99]).unnest()
    >>> levels = NameMatchLevel(levels_raw)
    >>> levels.as_string().execute()
    0    EXACT
    1     ELSE
    2     NEAR
    3     None
    Name: NameMatchLevel, dtype: object
    >>> levels.as_integer().name("levels").execute()
    0     0
    1     2
    2     1
    3    99
    Name: levels, dtype: int8

    Comparisons work as you expect:

    >>> NameMatchLevel.NEAR == 1
    True
    >>> NameMatchLevel(1) == "NEAR"
    True
    >>> (levels_raw == NameMatchLevel.NEAR).name("eq").execute()
    0    False
    1    False
    2     True
    3    False
    Name: eq, dtype: bool

    However, implicit ordering is not supported
    (file an issue if you think it should be):

    >>> NameMatchLevel.NEAR > 0
    Traceback (most recent call last):
    ...
    TypeError: '>' not supported between instances of 'NameMatchLevel' and 'int'
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
        if isinstance(value, str):
            self._check_str(value, ValueError)
        elif isinstance(value, int):
            self._check_int(value, ValueError)
        if isinstance(value, MatchLevel):
            self._value = value._value
        else:
            self._value = value

    def as_integer(self) -> int | ir.IntegerValue:
        """Convert to a python int or ibis integer, depending on the original type."""
        if _is_inty(self._value):
            return self._value
        return self._str_to_int(self._value)

    def as_string(self) -> str | ir.StringValue:
        """Convert to a python str or ibis string, depending on the original type."""
        if _is_stringy(self._value):
            return self._value
        return self._int_to_str(self._value)

    def __repr__(self) -> str:
        if _is_python(self._value):
            return self.__class__.__name__ + "." + str(self)
        else:
            return self.__class__.__name__ + "(<" + self._value.get_name() + ">)"

    def __str__(self) -> str:
        if not _is_python(self._value):
            raise TypeError(
                "You can only only use str() on MatchLevels made from str's and int's. "
                f"This {self.__class__.__name__} is made of {self._value.__class__.__name__}"  # noqa: E501
            )
        return self.as_string()

    def __int__(self) -> int:
        if not _is_python(self._value):
            raise TypeError(
                "You can only only use int() on MatchLevels made from str's and int's. "
                f"This {self.__class__.__name__} is made of {self._value.__class__.__name__}"  # noqa: E501
            )
        return self.as_integer()

    def __eq__(
        self, other: int | str | ir.NumericValue | ir.StringValue | MatchLevel
    ) -> bool | ir.BooleanValue:
        """"""
        if isinstance(other, MatchLevel):
            other = other._value
        if _is_inty(other):
            return self.as_integer() == other
        elif _is_stringy(other):
            return self.as_string() == other
        else:
            return NotImplemented

    @classmethod
    def _str_to_int(cls, v: str | ir.StringValue) -> int | ir.IntegerValue:
        # assumes v is in __s2i__
        if isinstance(v, str):
            cls._check_str(v, KeyError)
            return cls.__s2i__[v]
        elif isinstance(v, ir.StringValue):
            return v.substitute(cls.__s2i__, else_=ibis.null()).name(cls.__name__)
        assert False

    @classmethod
    def _int_to_str(cls, v: int | ir.IntegerValue) -> str | ir.StringValue:
        # assumes v is in __i2s__
        if isinstance(v, int):
            cls._check_int(v, KeyError)
            return cls.__i2s__[v]
        elif isinstance(v, ir.IntegerValue):
            return v.substitute(cls.__i2s__, else_=ibis.null()).name(cls.__name__)
        assert False

    @classmethod
    def _check_int(cls, value: int, err_type) -> None:
        valid_ints = set(cls.__i2s__.keys())
        if value not in valid_ints:
            raise err_type(f"Invalid value: {value}. Must be one of {valid_ints}")

    @classmethod
    def _check_str(cls, value: str, err_type) -> None:
        valid_strs = set(cls.__s2i__.keys())
        if value not in valid_strs:
            raise err_type(f"Invalid value: {value}. Must be one of {valid_strs}")


def _is_python(v):
    return isinstance(v, (int, str))


def _is_inty(v):
    return isinstance(v, (int, ir.NumericValue))


def _is_stringy(v):
    return isinstance(v, (str, ir.StringValue))


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
