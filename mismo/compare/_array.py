from __future__ import annotations

import ibis
from ibis.expr.types import ArrayValue, IntegerValue


def array_overlap_n(a: ArrayValue, b: ArrayValue) -> IntegerValue:
    """The number of elements shared by two arrays."""
    length_separate = a.length() + b.length()
    length_shared = a.union(b).unique().length()
    return length_separate - length_shared


def array_overlap_norm(a: ArrayValue, b: ArrayValue) -> IntegerValue:
    """
    The number of elements shared by two arrays, normalized to be between 0 and 1

    This is simply `array_overlap_n(a, b) / ibis.least(a.length(), b.length())`"""
    return array_overlap_n(a, b) / ibis.least(a.length(), b.length())


a = ibis.array([1, 2, 3, 4])
b = ibis.array([3, 4, 5, 6, 7, 8])
assert array_overlap_n(a, b).execute() == 2
assert array_overlap_norm(a, b).execute() == 1 / 2
