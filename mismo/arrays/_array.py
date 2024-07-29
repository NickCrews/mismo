from __future__ import annotations

from typing import Callable

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util


def array_combinations(left: ir.ArrayValue, right: ir.ArrayValue) -> ir.ArrayValue:
    """Generate all combinations of elements from two arrays.

    This is the cartesian product of the two arrays.

    Parameters
    ----------
    left
        The first array.
    right
        The second array.

    Returns
    -------
    combinations
        An `array<struct<l: T, r: U>>` where `T` is the type of the
        elements in `array1` and `U` is the type of the elements in `array2`.
    """
    return left.map(
        lambda le: right.map(lambda r: ibis.struct(dict(l=le, r=r)))
    ).flatten()


def array_filter_isin_other(
    t: ir.Table,
    array: ir.ArrayColumn | str,
    other: ir.Column,
    *,
    result_format: str = "{name}_filtered",
) -> ir.Table:
    """
    Equivalent to `t.mutate(result_name=t[array].filter(lambda x: x.isin(other)))`

    We can't have subqueries in the filter lambda (need this to avoid
    [https://stackoverflow.com/questions/77559936/how-to-implementlist-filterarray-elem-elem-in-column-in-other-table]()))

    See [issues/32](https://github.com/NickCrews/mismo/issues/32) for more info.

    Parameters
    ----------
    t :
        The table containing the array column.
    array :
        A reference to the array column.
    other :
        The column to filter against.
    result_format :
        The format string to use for the result column name. The format string
        should have a single placeholder, `{name}`, which will be replaced with
        the name of the array column.

    Returns
    -------
    ir.Table
        The table with a new column named following `result_format` with the
        filtered array.
    """  # noqa E501
    array_col = _util.get_column(t, array)
    t = t.mutate(__array=array_col, __id=ibis.row_number())
    temp = t.select("__id", __unnested=_.__array.unnest())
    # When we re-.collect() items below, the order matters,
    # but the .filter() can mess up the order, so we need to
    # add a sortable key, filter, re-sort, and then drop the key.
    filtered = (
        temp.mutate(elem_id=ibis.row_number())
        .filter(temp.__unnested.isin(other) | temp.__unnested.isnull())
        .order_by("elem_id")
        .drop("elem_id")
    )
    # NULLs are dropped from Array.collect() in ibis
    # https://github.com/ibis-project/ibis/issues/9703
    # So we can't do this:
    # re_agged = filtered.group_by("__id").agg(__filtered=_.__unnested.collect())
    # Instead, we have to do some raw SQL:
    uname = _util.unique_name("__filtered")
    re_agged = filtered.alias(uname).sql(
        f"SELECT __id, list(__unnested) as __filtered FROM {uname} GROUP BY __id",
        dialect="duckdb",
    )
    re_joined = t.left_join(re_agged, "__id").drop("__id", "__id_right")
    # when we unnested, both [] and NULL rows disappeared.
    # Once we join back, they all come back as NULL,
    # so we need to adjust some of them back to [].
    re_joined = re_joined.mutate(
        __filtered=(_.__array.notnull() & _.__filtered.isnull()).ifelse(
            [], _.__filtered
        )
    ).drop("__array")
    result_name = result_format.format(name=array_col.get_name())
    return re_joined.rename({result_name: "__filtered"})


def _list_select(x: ir.ArrayValue, indexes: ir.ArrayValue) -> ir.ArrayValue:
    """Selects elements from a list by index."""

    t = x.type()
    # if t.is_array():
    #     raise ValueError(f"Expected an array, got {t}")

    @ibis.udf.scalar.builtin(name="list_select", signature=((t, "array<int>"), t))
    def f(array, idxs): ...

    return f(x, indexes)


@ibis.udf.scalar.builtin(
    name="list_grade_up",
    signature=(("array<float64>",), "array<int64>"),
)
def _list_grade_up(x):
    """Works like sort, but returns the indexes instead of the actual values."""


def array_shuffle(a: ir.ArrayValue) -> ir.ArrayValue:
    """Shuffle an array."""
    idxs = a.map(lambda x: ibis.random())
    return _list_select(a, _list_grade_up(idxs))


def array_choice(a: ir.ArrayValue, n: int) -> ir.ArrayValue:
    """Randomly select `n` elements from an array."""
    return array_shuffle(a)[n:]


def array_sort(
    arr: ir.ArrayValue, *, key: Callable[[ir.Value], ir.Value] | None = None
) -> ir.ArrayValue:
    """Sort an array, optionally using a key function.

    The builtin ArrayValue.sort() method doesn't support a key function.
    This function is a workaround for that.

    See https://github.com/duckdb/duckdb/discussions/10417
    """
    if key is None:
        return arr.sort()
    keys = arr.map(key)
    return _list_select(arr, _list_grade_up(keys))
