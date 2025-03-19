"""Vector operations"""

from __future__ import annotations

from typing import Literal, TypeVar

import ibis
import ibis.expr.datatypes as dt
import ibis.expr.types as ir


@ibis.udf.scalar.builtin(name="array_sum")
def _array_sum(a) -> float: ...


# for duckdb
@ibis.udf.scalar.builtin(name="list_dot_product")
def _array_dot_product(a, b) -> float: ...


# for duckdb
@ibis.udf.scalar.builtin(name="list_cosine_similarity")
def _array_cosine_similarity(a, b) -> float: ...


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
    >>> dot(v1, v2).execute()  # 1*4 + 2*5
    14.0
    >>> m1 = ibis.map({"a": 1, "b": 2})
    >>> m2 = ibis.map({"b": 3, "c": 4})
    >>> dot(m1, m2).execute()  # 2*3
    6.0
    """
    a_vals, b_vals = _shared_vals(a, b)
    return _array_dot_product(a_vals, b_vals)


def cosine_similarity(a: T, b: T) -> ir.FloatingValue:
    """Compute the cosine similarity of two vectors

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
        The cosine similarity of the two vectors.

    Examples
    --------
    >>> import ibis
    >>> from mismo.vector import cosine_similarity

    Opposite directions:

    >>> cosine_similarity(
    ...     ibis.array([1, 1]), ibis.array([-2, -2])
    ... ).execute()  # doctest: +SKIP
    -1.0

    Orthogonal vectors:

    >>> cosine_similarity(ibis.array([1, 0]), ibis.array([0, 1])).execute()
    0.0
    """  # noqa: E501
    a_vals, b_vals = _shared_vals(a, b)
    return _array_cosine_similarity(a_vals, b_vals)


def mul(a: T, b: T) -> T:
    """Element-wise multiplication of two vectors"""
    if isinstance(a, ir.ArrayValue) and isinstance(b, ir.ArrayValue):
        return a.zip(b).map(lambda struct: struct.f1 * struct.f2)
    elif isinstance(a, ir.MapValue) and isinstance(b, ir.MapValue):
        keys = _shared_keys(a, b)
        vals = keys.map(lambda k: a[k] * b[k])
        is_null = vals.isnull()
        result = map_(keys.fill_null([]), vals.fill_null([]))
        return is_null.ifelse(ibis.null(), result)
    else:
        raise ValueError(f"Unsupported types {type(a)} and {type(b)}")


def norm(vec: T, *, metric: Literal["l1", "l2"] = "l2") -> ir.FloatingValue:
    """Compute the norm (length) of a vector.

    The vector can either be a dense vector, represented as array<numeric>,
    or a sparse vector, represented as map<any_type, numeric>.

    Parameters
    ----------
    vec :
        The vector to compute the norm of.
    metric : {"l1", "l2"}, default "l2"
        The metric to use. "l1" for Manhattan distance, "l2" for Euclidean distance.

    Returns
    -------
    The norm of the vector.

    Examples
    --------
    >>> import ibis
    >>> from mismo.vector import norm
    >>> v = ibis.array([-3, 4])
    >>> norm(v).execute()
    5.0
    >>> m = ibis.map({"a": -3, "b": 4})
    >>> norm(m, metric="l1").execute()
    7.0
    """
    if isinstance(vec, ir.ArrayValue):
        vals = vec
    elif isinstance(vec, ir.MapValue):
        vals = map_values(vec)
    else:
        raise ValueError(f"Unsupported type {type(vec)}")

    if metric == "l1":
        return _array_sum(vals.map(lambda x: x.abs()))
    elif metric == "l2":
        return _array_sum(vals.map(lambda x: x**2)).sqrt()
    else:
        raise ValueError(f"Unsupported norm {metric}")


def normalize(vec: T, *, metric: Literal["l1", "l2"] = "l2") -> T:
    """Normalize a vector to have unit length.

    The vector can either be a dense vector, represented as array<numeric>,
    or a sparse vector, represented as map<any_type, numeric>.
    The returned vector will have the same type as the input vector.

    Parameters
    ----------
    vec :
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
    >>> from mismo.vector import normalize
    >>> normalize(ibis.array([1, 2])).execute()
    [0.4472135954999579, 0.8944271909999159]
    >>> normalize(ibis.array([1, 2]), metric="l1").execute()
    [0.3333333333333333, 0.6666666666666666]
    >>> normalize(ibis.map({"a": 1, "b": 2})).execute()
    {'a': 0.4472135954999579, 'b': 0.8944271909999159}
    """
    if isinstance(vec, ir.ArrayValue):
        vals = vec
    elif isinstance(vec, ir.MapValue):
        vals = map_values(vec)
    else:
        raise ValueError(f"Unsupported type {type(vec)}")

    denom = norm(vec, metric=metric)
    normed_vals = vals.map(lambda x: x / denom)

    if isinstance(vec, ir.ArrayValue):
        return normed_vals
    else:
        return map_(map_keys(vec), normed_vals)


def _shared_keys(a: ir.MapValue, b: ir.MapValue) -> ir.ArrayValue:
    regular = map_keys(a).filter(lambda k: b.contains(k))
    null = ibis.literal(None, type=dt.Array(value_type=a.type().key_type))
    return b.isnull().ifelse(null, regular)


def _shared_vals(a: T, b: T) -> tuple[ir.ArrayValue, ir.ArrayValue]:
    if isinstance(a, ir.ArrayValue) and isinstance(b, ir.ArrayValue):
        return a, b
    elif isinstance(a, ir.MapValue) and isinstance(b, ir.MapValue):
        keys = _shared_keys(a, b)
        a_vals = keys.map(lambda k: a[k])
        b_vals = keys.map(lambda k: b[k])
        return a_vals, b_vals
    else:
        raise ValueError(f"Unsupported types {type(a)} and {type(b)}")


def map_keys(m: ir.MapValue) -> ir.ArrayValue:
    """workaround for https://github.com/duckdb/duckdb/issues/11116"""
    normal = m.keys()
    if not _need_old_duckdb_workaround():
        return normal
    null = ibis.literal(None, type=dt.Array(value_type=m.type().key_type))
    return m.isnull().ifelse(null, normal)


def map_values(m: ir.MapValue) -> ir.ArrayValue:
    """workaround for https://github.com/duckdb/duckdb/issues/11116"""
    normal = m.values()
    if not _need_old_duckdb_workaround():
        return normal
    null = ibis.literal(None, type=dt.Array(value_type=m.type().value_type))
    return m.isnull().ifelse(null, normal)


def map_(keys: ir.ArrayValue, values: ir.ArrayValue) -> ir.MapValue:
    """workaround for https://github.com/duckdb/duckdb/issues/11115"""
    if not _need_old_duckdb_workaround():
        return ibis.map(keys, values)
    either_null = keys.isnull() | values.isnull()
    regular = ibis.map(keys, values)
    null = ibis.literal(
        None,
        type=dt.Map(
            key_type=keys.type().value_type, value_type=values.type().value_type
        ),
    )
    return either_null.ifelse(null, regular)


def _need_old_duckdb_workaround() -> bool:
    # Do we need workarounds for:
    # - https://github.com/duckdb/duckdb/issues/11115
    # - https://github.com/duckdb/duckdb/issues/11116
    # They were fixed in
    # https://github.com/duckdb/duckdb/releases/tag/v0.10.3
    try:
        import duckdb
    except ImportError:
        return False
    return duckdb.__version__ < "0.10.3"
