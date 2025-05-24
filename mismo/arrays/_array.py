from __future__ import annotations

from collections.abc import Iterable
from typing import Callable

import ibis
from ibis import _
from ibis.expr import datatypes as dt
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


def array_combinations_first_n(
    left: ir.ArrayValue,
    right: ir.ArrayValue,
    *,
    n_left: int,
    n_right: int,
) -> list[tuple[ir.Value, ir.Value]]:
    """Generate the combinations from the first N elements of two arrays.

    This is a static version of `array_combinations`.
    That function generates ALL combinations of the two arrays,
    but since that number is dynamic, it can be hard to work with.
    This function is useful for if you have a small number of
    combinations you want to generate,
    eg you want to compare all the email addresses for
    one person to all the email addresses for another person.

    Parameters
    ----------
    left
        The first array.
    right
        The second array.
    n_left
        The number of elements to take from the first array.
    n_right
        The number of elements to take from the second array.

    Returns
    -------
    combinations
        A list of (left, right) tuples.
    """
    if n_left < 0 or n_right < 0:
        raise ValueError("n_left and n_right must be non-negative.")
    results = []
    for i in range(n_left):
        for j in range(n_right):
            results.append((left[i], right[j]))
    return results


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
    # add a sortable key before the filter
    filtered = temp.mutate(elem_id=ibis.row_number()).filter(
        temp.__unnested.isin(other) | temp.__unnested.isnull()
    )
    re_agged = filtered.group_by("__id").agg(
        __filtered=filtered.__unnested.collect(include_null=True, order_by="elem_id")
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
    # signature=(("array<float64>",), "array<int64>"),
)
def _list_grade_up(x) -> dt.Array(value_type=dt.Int64()):
    """Works like sort, but returns the indexes instead of the actual values."""


def array_shuffle(a: ir.ArrayValue) -> ir.ArrayValue:
    """Shuffle an array."""
    idxs = a.map(lambda x: ibis.random())
    return _list_select(a, _list_grade_up(idxs))


def array_choice(a: ir.ArrayValue, n: int) -> ir.ArrayValue:
    """Randomly select `n` elements from an array."""
    return array_shuffle(a)[n:]


def array_sort(
    arr: ir.ArrayValue,
    /,
    *,
    key: ibis.Deferred
    | Callable[[ir.Value], ir.Value | Iterable[ir.Value]]
    | Iterable[ibis.Deferred]
    | None = None,
) -> ir.ArrayValue:
    """Sort an array, optionally using a key.

    The builtin ArrayValue.sort() method doesn't support a key.
    This is a workaround for that.

    See https://github.com/duckdb/duckdb/discussions/10417
    """
    if key is None:
        return arr.sort()

    def resolve_key(elem: ir.Value, k):
        if isinstance(k, ir.Value):
            return k
        if isinstance(k, ibis.Deferred):
            return k.resolve(elem)
        if callable(k):
            return resolve_key(elem, k(elem))
        # assume iterable of values
        return ibis.struct(
            {f"f{i}": resolve_key(elem, subkey) for i, subkey in enumerate(k)}
        )

    keys = arr.map(lambda x: resolve_key(x, key))
    return _list_select(arr, _list_grade_up(keys))
