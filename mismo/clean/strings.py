from __future__ import annotations

from ibis.expr.types import (
    ArrayValue,
    StringValue,
)


def norm_whitespace(texts: StringValue) -> StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore


def norm_possessives(texts: StringValue) -> StringValue:
    """Fix "jane's house" to "janes house".

    TODO: Also deal with "Ross' house"
    """
    return texts.re_replace(r"(\w)\'s(\b)", r"\1s\2")  # type: ignore


def tokenize(texts: StringValue) -> ArrayValue:
    """Returns an column where each element is an array of strings"""
    return norm_whitespace(texts).split(" ")
