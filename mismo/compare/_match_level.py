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
        newcls.__members__ = {s: newcls(s) for s in s2i}
        for k, v in newcls.__members__.items():
            setattr(newcls, k, v)
        newcls.__annotations__ = {
            **newcls.__annotations__,
            **{k: newcls for k in s2i},
        }
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


class MatchLevel(metaclass=_LevelsMeta):
    """An enum-like class for match levels."""

    def __init__(self, value: ir.StringValue | ir.IntegerValue):
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
