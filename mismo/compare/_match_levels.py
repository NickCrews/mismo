from __future__ import annotations

from abc import ABCMeta
import typing

import ibis
from ibis.expr import types as ir


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
            **newcls.__annotations__,
            **{k: newcls for k in s2i},
        }
        for s in s2i:
            setattr(newcls, s, newcls(s))
        return newcls

    @typing.overload
    def __getitem__(self, key: str) -> int: ...

    @typing.overload
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

    def __iter__(self) -> typing.Iterator[str]:
        return iter(self.__s2i__.keys())

    def __len__(self) -> int:
        return len(self.__s2i__)

    def __repr__(self) -> str:
        pairs = (f"{k}={v}" for k, v in self.__s2i__.items())
        return f"{self.__name__}({', '.join(pairs)})"


class MatchLevels(metaclass=_LevelsMeta):
    """An enum-like class for match levels.

    This class is used to define the levels of agreement between two records.

    Examples
    --------
    >>> from mismo.compare import MatchLevels
    >>> class NameMatchLevels(MatchLevels):
    ...     EXACT = 0
    ...     NEAR = 1
    ...     ELSE = 2
    ...
    >>> NameMatchLevels.EXACT
    EXACT
    >>> NameMatchLevels.EXACT.as_string()
    'EXACT'
    >>> NameMatchLevels.EXACT.as_integer()
    0
    >>> len(NameMatchLevels)
    3
    >>> 2 in NameMatchLevels
    True
    >>> NameMatchLevels[1]
    'NEAR'

    You can construct your own values:

    >>> NameMatchLevels("NEAR").as_integer()
    1
    >>> NameMatchLevels(2).as_string()
    'ELSE'
    >>> NameMatchLevels(3) # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    ValueError: Invalid value: 3. Must be one of {0, 1, 2}

    The powerful thing is it can be used to convert between string and integer
    ibis expressions:

    >>> import ibis
    >>> levels_raw = ibis.array([0, 2, 1, 99]).unnest()
    >>> levels = NameMatchLevels(levels_raw)
    >>> levels.as_string().execute()
    0    EXACT
    1     ELSE
    2     NEAR
    3     None
    Name: NameMatchLevels, dtype: object
    >>> levels.as_integer().execute()
    0     0
    1     2
    2     1
    3    99
    Name: Array(), dtype: int8
    """

    def __init__(self, value: int | str | ir.StringValue | ir.IntegerValue):
        """Create a new match level value.

        If the given value is a python int or str, it is checked against the
        valid values for this class. If it is an ibis expression,
        we do no such check.

        Parameters
        ----------
        value :
            The value of the match level.
        """
        if not isinstance(value, (ir.StringValue, ir.IntegerValue, int, str)):
            raise TypeError(f"Invalid value: {value}")
        valid_ints = set(self.__i2s__.keys())
        valid_strs = set(self.__s2i__.keys())
        if isinstance(value, int) and value not in valid_ints:
            raise ValueError(f"Invalid value: {value}. Must be one of {valid_ints}")
        if isinstance(value, str) and value not in valid_strs:
            raise ValueError(f"Invalid value: {value}. Must be one of {valid_strs}")
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
        if isinstance(other, MatchLevels):
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
