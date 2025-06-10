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

    _t: ibis.Table
    """The underlying Table that this is a proxy for."""

    def __init__(self, t: ibis.Table) -> None:
        object.__setattr__(self, "_t", t)

    def __getattr__(self, key: str):
        try:
            object.__getattr__(self, key)
        except AttributeError:
            pass
        return getattr(self._t, key)
