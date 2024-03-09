"""Vector operations"""
from __future__ import annotations

from typing import Literal, TypeVar

import ibis
import ibis.expr.types as ir


@ibis.udf.scalar.builtin(name="array_sum")
def _array_sum(a) -> float:
    ...


# for duckdb
@ibis.udf.scalar.builtin(name="list_dot_product")
def _array_dot_product(a, b) -> float:
    ...


T = TypeVar("T", ir.MapValue, ir.ArrayValue)


def dot(a: T, b: T) -> ir.FloatingValue:
    """Compute the dot product of two vectors

    The vectors can either be dense vectors, represented as array<numeric>,
    or sparse vectors, represented as map<any_type, numeric>.
    Both vectors must be of the same type though.

    Parameters
    ----------
    a :
        The first vector.
    b :
        The second vector.

    Returns
    -------
    FloatingValue
        The dot product of the two vectors.

    Examples
    --------
    >>> import ibis
    >>> from mismo.vector import dot
    >>> v1 = ibis.array([1, 2])
    >>> v2 = ibis.array([4, 5])
    >>> dot(v1, v2)
    14.0  # 1*4 + 2*5
    >>> m1 = ibis.map({"a": 1, "b": 2})
    >>> m2 = ibis.map({"b": 3, "c": 4})
    >>> dot(m1, m2)
    6.0  # 2*3
    """
    if isinstance(a, ir.ArrayValue) and isinstance(b, ir.ArrayValue):
        a_vals = a
        b_vals = b
    elif isinstance(a, ir.MapValue) and isinstance(b, ir.MapValue):
        keys = a.keys().intersect(b.keys())
        a_vals = keys.map(lambda k: a[k])
        b_vals = keys.map(lambda k: b[k])
    else:
        raise ValueError(f"Unsupported types {type(a)} and {type(b)}")
    return _array_dot_product(a_vals, b_vals)


def norm(arr: T, metric: Literal["l1", "l2"] = "l2") -> T:
    """Normalize a vector to have unit length.

    The vector can either be a dense vector, represented as array<numeric>,
    or a sparse vector, represented as map<any_type, numeric>.
    The returned vector will have the same type as the input vector.

    Parameters
    ----------
    arr :
        The vector to normalize.
    metric : {"l1", "l2"}, default "l2"
        The metric to use. "l1" for Manhattan distance, "l2" for Euclidean distance.

    Returns
    -------
    ArrayValue
        The normalized vector.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> from mismo.vector import norm
    >>> norm(ibis.array([1, 2]))
    [0.4472135954999579, 0.8944271909999159]
    >>> norm(ibis.array([1, 2]), "l1")
    [0.3333333333333333, 0.6666666666666666]
    >>> norm(ibis.map({"a": 1, "b": 2}))
    {"a": 0.4472135954999579, "b": 0.8944271909999159}
    """
    if isinstance(arr, ir.ArrayValue):
        vals = arr
    elif isinstance(arr, ir.MapValue):
        vals = arr.values()
    else:
        raise ValueError(f"Unsupported type {type(arr)}")

    if metric == "l1":
        denom = _array_sum(vals)
    elif metric == "l2":
        denom = _array_sum(vals.map(lambda x: x**2)).sqrt()
    else:
        raise ValueError(f"Unsupported norm {metric}")
    normed_vals = vals.map(lambda x: x / denom)

    if isinstance(arr, ir.ArrayValue):
        return normed_vals
    else:
        return ibis.map(arr.keys(), normed_vals)
