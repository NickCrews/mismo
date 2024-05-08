from __future__ import annotations

from typing import Literal

import ibis
from ibis.expr import types as ir

from mismo._array import array_combinations, array_min
from mismo._util import get_column
from mismo.compare import MatchLevels
from mismo.text import damerau_levenshtein


def parse_and_normalize_email(email: ir.StringValue) -> ir.StructValue:
    """Parses an email address into its `<user>@<domain>` parts.

    The email address is normalized by removing dots and underscores,
    and converting to lowercase.

    Parameters
    ----------
    email :
        The email address.

    Returns
    -------
    parsed : StructValue<full: string, user: string, domain: string>
        The parsed and normalized email address.

    Examples
    --------
    import ibis
    >>> from mismo.lib.email import parse_and_normalize_email
    >>> parse_and_normalize_email(ibis.literal(" Bob.Jones@gmail.com")).execute()
    {'full': 'bobjones@gmailcom', 'user': 'bobjones', 'domain': 'gmailcom'}
    """
    email = (
        email.strip().lower().replace(".", "").replace("_", "").nullif("").nullif("@")
    )
    parts = email.split("@")
    user, domain = parts[0].nullif(""), parts[1].nullif("")
    return ir.struct({"full": email, "user": user, "domain": domain})


class EmailMatchLevel(MatchLevels):
    """How closely two email addresses of the form `<user>@<domain>` match.

    Case is ignored, and dots and underscores are removed.
    """

    FULL_EXACT = 0
    """The full email addresses are exactly the same."""
    FULL_NEAR = 1
    """The full email addresses have a small edit distance."""
    USER_EXACT = 2
    """The user part of the email addresses are exactly the same."""
    USER_NEAR = 3
    """The user part of the email addresses have a small edit distance."""
    ELSE = 4
    """None of the above."""


def match_level(
    e1: ir.StructValue | ir.StringValue,
    e2: ir.StructValue | ir.StringValue,
    *,
    native_representation: Literal["integer", "string"] = "integer",
) -> EmailMatchLevel:
    """Match level of two email addresses.

    Parameters
    ----------
    e1 :
        The first email address. If a string, it will be parsed and normalized.
    e2 :
        The second email address. If a string, it will be parsed and normalized.

    Returns
    -------
    level : EmailMatchLevel
        The match level.
    """
    if isinstance(e1, ir.StringValue):
        e1 = parse_and_normalize_email(e1)
    if isinstance(e2, ir.StringValue):
        e2 = parse_and_normalize_email(e2)

    def f(level: MatchLevels):
        if native_representation == "string":
            return level.as_string()
        else:
            return level.as_integer()

    raw = (
        ibis.case()
        .when(e1 == e2, f(EmailMatchLevel.FULL_EXACT))
        .when(damerau_levenshtein(e1.full, e2.full) <= 1, f(EmailMatchLevel.FULL_NEAR))
        .when(e1.user == e2.user, f(EmailMatchLevel.USER_EXACT))
        .when(damerau_levenshtein(e1.user, e2.user) <= 1, f(EmailMatchLevel.USER_NEAR))
        .else_(f(EmailMatchLevel.ELSE))
        .end()
    )
    return EmailMatchLevel(raw)


class EmailsDimension:
    """A dimension of email addresses."""

    def __init__(
        self,
        column: str,
        *,
        column_parsed: str = "{column}_parsed",
        column_compared: str = "{column}_compared",
    ):
        """Initialize the dimension.

        Parameters
        ----------
        column :
            The name of the column that holds a array<string> of email addresses.
        column_parsed :
            The name of the column that will be filled with the parsed email addresses.
        column_compared :
            The name of the column that will be filled with the comparison results.
        """
        self.column = column
        self.column_parsed = column_parsed.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prep(self, t: ir.Table) -> ir.Table:
        """Add a column with the parsed and normalized email addresses."""
        return t.mutate(
            get_column(t, self.column)
            .map(parse_and_normalize_email)
            .name(self.column_parsed)
        )

    def compare(self, t: ir.Table) -> ir.Table:
        """Add a column with the best match between all pairs of email addresses."""
        le = t[self.column_parsed + "_l"]
        ri = t[self.column_parsed + "_r"]
        pairs = array_combinations(le, ri)
        min_level = array_min(
            pairs.map(lambda pair: match_level(pair.a1, pair.a2).as_integer())
        )
        return t.mutate(
            EmailMatchLevel(min_level).as_string().name(self.column_compared)
        )
