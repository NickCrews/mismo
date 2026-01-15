from __future__ import annotations

import ibis


class TableWrapper(ibis.Table):
    """A wrapper around an ibis Table that allows you to access its attributes.

    This allows the user to pretend that they are working with an ibis Table:

    - `isinstance(obj, ibis.Table)` is True
    - users can call all normal methods `t.distinct()`
    - can access columns, eg `t.column_name` or `t["column_name"]`

    but it also allows use as developers to extend the functionality of the table,
    eg adding custom methods or attributes.
    """

    __wrapped__: ibis.Table
    """The underlying Table that this is a proxy for."""

    def __init__(self, wrapped: ibis.Table, /) -> None:
        object.__setattr__(self, "__wrapped__", wrapped)

    def __getattr__(self, key: str) -> ibis.Column:
        return getattr(self.__wrapped__, key)


class StructWrapper(ibis.ir.StructValue):
    """A wrapper around an ibis StructValue that allows you to access its attributes.

    This allows the user to pretend that they are working with an ibis StructValue:

    - `isinstance(obj, ibis.ir.StructValue)` is True
    - users can call all normal methods `v.isnull()`
    - can access fields, eg `v.my_field` or `v["my_field"]`

    but it also allows use as developers to extend the functionality of the struct,
    eg adding custom methods or attributes.
    """

    __slots__ = ("__wrapped__",)
    __wrapped__: ibis.ir.StructValue
    """The underlying StructValue that this is a proxy for."""

    def __init__(self, wrapped: ibis.ir.StructValue, /) -> None:
        object.__setattr__(self, "__wrapped__", wrapped)

    def __getattr__(self, name: str, /) -> ibis.Value:
        return getattr(self.__wrapped__, name)
