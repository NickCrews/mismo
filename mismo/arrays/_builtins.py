from __future__ import annotations

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

_ARRAY_AGGS = {}


def _get_array_agg(array: ir.ArrayValue, name: str, *, out_type=None) -> ir.Column:
    t = array.type()
    if not isinstance(array, ir.ArrayValue):
        raise ValueError(f"Expected an array, got {t}")

    if out_type is None:
        out_type = t.value_type()

    key = (t, name)
    if key not in _ARRAY_AGGS:

        @ibis.udf.scalar.builtin(name=name, signature=((t,), out_type))
        def f(array): ...

        _ARRAY_AGGS[key] = f

    return _ARRAY_AGGS[key](array)


def array_min(array: ir.ArrayValue) -> ir.NumericValue:
    """Get the minimum value of an array."""
    return _get_array_agg(array, "list_min")


def array_sum(array: ir.ArrayValue) -> ir.NumericValue:
    """Get the sum of all values of an array."""
    return _get_array_agg(array, "list_sum")


def array_max(array: ir.ArrayValue) -> ir.NumericValue:
    """Get the maximum value of an array."""
    return _get_array_agg(array, "list_max")


# @ibis.udf.scalar.builtin(name="list_avg")
def array_mean(array: ir.ArrayValue) -> ir.FloatingValue:
    """Get the mean value of an array."""
    return _get_array_agg(array, "list_avg", out_type=dt.float64)


# @ibis.udf.scalar.builtin(name="list_median")
def array_median(array: ir.ArrayValue) -> ir.FloatingValue:
    """Get the median value of an array."""
    return _get_array_agg(array, "list_median", out_type=dt.float64)


@ibis.udf.scalar.builtin(name="list_bool_or")
def array_any(array) -> bool:
    """Return True if any elements in the array are True, False otherwise.

    NULL values are ignored.
    If there are no non-NULL values, returns NULL.
    """


@ibis.udf.scalar.builtin(name="list_bool_and")
def array_all(array) -> bool:
    """Return True if all elements in the array are True, False otherwise.

    NULL values are ignored.
    If there are no non-NULL values, returns NULL.
    """
