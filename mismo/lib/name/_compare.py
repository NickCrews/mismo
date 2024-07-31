from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util
from mismo.compare import MatchLevel
from mismo.lib.name._nicknames import are_aliases
from mismo.text import damerau_levenshtein


def are_match_with_nicknames(
    left: ir.StructValue, right: ir.StructValue
) -> ir.BooleanValue:
    """The given names match via nickname or alias, and the surname names match."""
    return ibis.and_(
        are_aliases(left.given, right.given),
        left.surname == right.surname,
    )


def initials_equal(left: ir.StringValue, right: ir.StringValue) -> ir.BooleanValue:
    """The first letter matches, and at least one is a single letter."""
    return ibis.and_(
        left[0] == right[0],
        ibis.or_(right.length() == 1, left.length() == 1),
    )


def are_spelling_error(
    left: ir.StringValue,
    right: ir.StringValue,
) -> ir.BooleanValue:
    edit_distance = damerau_levenshtein(left, right)
    return ibis.or_(
        edit_distance <= 1,
        ibis.and_(edit_distance <= 2, left.length() >= 5),
        substring_match(left, right),
    )


def substring_match(
    left: ir.StringValue, right: ir.StringValue, *, min_len: int = 3
) -> ir.BooleanValue:
    """The shorter string is a substring of the longer string, and at least min_len."""
    return ibis.or_(
        ibis.and_(left.contains(right), right.length() >= min_len),
        ibis.and_(right.contains(left), left.length() >= min_len),
    )


class NameMatchLevel(MatchLevel):
    """How closely two names match."""

    NULL = 0
    """At least one given or surname is NULL from either side."""
    EXACT = 1
    """The names are exactly the same."""
    GIVEN_SURNAME = 2
    """The given and surnames both match."""
    NICKNAMES = 3
    """The given names match with nicknames, and the surnames match."""
    INITIALS = 4
    """The first letter of the given name matches, and the surnames match."""
    ELSE = 5
    """None of the above."""


class NameComparer:
    """Compare names."""

    def __init__(
        self,
        left_column: str = "name_l",
        right_column: str = "name_r",
        *,
        result_column: str | None = None,
    ):
        if result_column is None:
            result_column = f"{left_column}_vs_{right_column}"
        self.left_column = left_column
        self.right_column = right_column
        self.name = result_column

    def __call__(self, pairs: ir.Table) -> ir.Table:
        """Compare pairs of names.

        Parameters
        ----------
        pairs :
            A table with columns ``self.left_column`` and ``self.right_column``.

        Returns
        -------
        t :
            The table with the comparison results in the column ``self.name``.
        """
        le = pairs[self.left_column]
        ri = pairs[self.right_column]
        result = _util.cases(
            (
                ibis.or_(
                    _util.struct_isnull(le, how="any", fields=["given", "surname"]),
                    _util.struct_isnull(ri, how="any", fields=["given", "surname"]),
                ),
                NameMatchLevel.NULL.as_integer(),
            ),
            (_util.struct_equal(le, ri), NameMatchLevel.EXACT.as_integer()),
            (
                _util.struct_equal(le, ri, fields=["given", "surname"]),
                NameMatchLevel.GIVEN_SURNAME.as_integer(),
            ),
            (
                are_match_with_nicknames(le, ri),
                NameMatchLevel.NICKNAMES.as_integer(),
            ),
            (
                ibis.and_(
                    initials_equal(le.given, ri.given),
                    le.surname == ri.surname,
                ),
                NameMatchLevel.INITIALS.as_integer(),
            ),
            else_=(NameMatchLevel.ELSE.as_integer()),
        )
        return pairs.mutate(result.name(self.name))
