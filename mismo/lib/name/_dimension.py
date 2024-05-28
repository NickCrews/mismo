from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util
from mismo.compare import MatchLevels
from mismo.lib.name import _clean, _compare


class NameMatchLevels(MatchLevels):
    """How closely two names match."""

    NULL = 0
    """At least one first or last name is NULL from either side."""
    EXACT = 1
    """The names are exactly the same."""
    FIRST_LAST = 2
    """The first and last names match."""
    NICKNAMES = 3
    """The first names match with nicknames, and the last names match."""
    INITIALS = 4
    """The first letter of the first name matches, and the last names match."""
    ELSE = 5
    """None of the above."""


class NameDimension:
    """Preps, blocks, and compares based on a human name.

    A name is a Struct of the type
    `struct<
        prefix: string,
        first: string,
        middle: string,
        last: string,
        suffix: string,
        nickname: string,
    >`.
    """

    def __init__(
        self,
        column: str,
        *,
        column_normed: str = "{column}_normed",
        column_tokens: str = "{column}_tokens",
        column_compared: str = "{column}_compared",
    ) -> None:
        self.column = column
        self.column_normed = column_normed.format(column=column)
        self.column_tokens = column_tokens.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prep(self, t: ir.Table) -> ir.Table:
        """Add columns with the normalized name and name tokens.

        Parameters
        ----------
        t : ir.Table
            The table to prep.

        Returns
        -------
        t : ir.Table
            The prepped table.
        """
        t = t.mutate(_clean.normalize_name(t[self.column]).name(self.column_normed))
        # workaround for https://github.com/ibis-project/ibis/issues/8484
        t = t.cache()
        t = t.mutate(_clean.name_tokens(t[self.column_normed]).name(self.column_tokens))
        return t

    def compare(self, t: ir.Table) -> ir.Table:
        """Compare the left and right names.

        Parameters
        ----------
        t :
            The table to compare.

        Returns
        -------
        t :
            The compared table.
        """

        le = t[self.column_normed + "_l"]
        ri = t[self.column_normed + "_r"]
        result = (
            ibis.case()
            .when(
                ibis.or_(
                    _util.struct_isnull(le, how="any", fields=["first", "last"]),
                    _util.struct_isnull(ri, how="any", fields=["first", "last"]),
                ),
                NameMatchLevels.NULL.as_integer(),
            )
            .when(_util.struct_equal(le, ri), NameMatchLevels.EXACT.as_integer())
            .when(
                _util.struct_equal(le, ri, fields=["first", "last"]),
                NameMatchLevels.FIRST_LAST.as_integer(),
            )
            .when(
                _compare.are_match_with_nicknames(le, ri),
                NameMatchLevels.NICKNAMES.as_integer(),
            )
            .when(
                ibis.and_(
                    _compare.initials_equal(le["first"], ri["first"]),
                    le["last"] == ri["last"],
                ),
                NameMatchLevels.INITIALS.as_integer(),
            )
            .else_(NameMatchLevels.ELSE.as_integer())
            .end()
        )
        return t.mutate(result.name(self.column_compared))
