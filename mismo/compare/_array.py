from __future__ import annotations

import ibis
from ibis.expr.types import ArrayValue, IntegerValue


def intersection_n(a: ArrayValue, b: ArrayValue) -> IntegerValue:
    """The number of elements shared by two arrays.

    Parameters
    ----------
    a : ArrayValue
        The first array.
    b : ArrayValue
        The second array.

    Returns
    -------
    IntegerValue
        The number of elements shared by the two arrays.
    """
    length_total = a.length() + b.length()
    length_union = a.union(b).length()
    return length_total - length_union


def jaccard(a: ArrayValue, b: ArrayValue) -> IntegerValue:
    """The Jaccard similarity between two arrays.

    Parameters
    ----------
    a : ArrayValue
        The first array.
    b : ArrayValue
        The second array.

    Returns
    -------
    IntegerValue
        The Jaccard similarity between the two arrays.
    """
    length_total = a.length() + b.length()
    length_union = a.union(b).length()
    length_intersection = length_total - length_union
    return length_intersection / length_union


a = ibis.array([1, 2, 3, 4])
b = ibis.array([3, 4, 5, 6, 7, 8])
assert intersection_n(a, b).execute() == 2
assert jaccard(a, b).execute() == 1 / 4
