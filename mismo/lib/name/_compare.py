from __future__ import annotations

import ibis
from ibis.expr import types as it

from mismo.lib.name._nicknames import are_aliases


def are_match_with_nicknames(
    left: it.StructValue, right: it.StructValue
) -> it.BooleanValue:
    """The first names match via nickname or alias, and the last names match."""
    return ibis.and_(
        are_aliases(left["first"], right["first"]),
        left["last"] == right["last"],
    )


def initials_equal(left: it.StringValue, right: it.StringValue) -> it.BooleanValue:
    """The first letter matches, and at least one is a single letter."""
    return ibis.and_(
        left[0] == right[0],
        ibis.or_(right.length() == 1, left.length() == 1),
    )


@ibis.udf.scalar.builtin
def damerau_levenshtein(left: str, right: str) -> int:
    ...


def are_spelling_error(
    left: it.StringValue,
    right: it.StringValue,
) -> it.BooleanValue:
    edit_distance = damerau_levenshtein(left, right)
    return ibis.or_(
        edit_distance <= 1,
        ibis.and_(edit_distance <= 2, left.length() >= 5),
        substring_match(left, right),
    )


def substring_match(
    left: it.StringValue, right: it.StringValue, *, min_len: int = 3
) -> it.BooleanValue:
    """The shorter string is a substring of the longer string, and at least min_len."""
    return ibis.or_(
        ibis.and_(left.contains(right), right.length() >= min_len),
        ibis.and_(right.contains(left), left.length() >= min_len),
    )
