from __future__ import annotations

from typing import Literal

import ibis
from ibis.expr import types as ir

from mismo._util import cases, get_column
from mismo.arrays import array_combinations, array_min
from mismo.compare import MatchLevel
from mismo.linker import UnnestLinker
from mismo.text import damerau_levenshtein


def clean_email(email: ir.StringValue, *, normalize: bool = False) -> ir.StringValue:
    r"""Clean an email address.

    - convert to lowercase
    - extract anything that matches r".*(\S+@\S+).*"

    If ``normalize`` is True, an additional step of removing "." and "_" is performed.
    This makes it possible to compare two addresses and be more immune to noise.
    For example, in many email systems such as gmail, "." are ignored.
    """
    pattern = r"(\S+@\S+)"
    email = email.lower().re_extract(pattern, 1).nullif("")
    if normalize:
        email = email.replace(".", "").replace("_", "")
    return email


class ParsedEmail:
    """A simple data class holding an email address that has been split into parts."""

    full: ir.StringValue
    """The full email address, eg 'bob.smith@gmail.com'."""
    user: ir.StringValue
    """The user part of the email address, eg 'bob.smith' of 'bob.smith@gmail.com'"""
    domain: ir.StringValue
    """The domain part of the email address, eg 'gmail.com'."""

    def __init__(self, full: ir.StringValue, /):
        """Parse an email address from the full string.

        Does no cleaning or normalization. If you want that, use `clean_email` first.

        Parameters
        ----------
        full :
            The full email address.
        """
        self.full = full
        self.user = full.split("@")[0].nullif("")
        self.domain = full.split("@")[1].nullif("")

    def as_struct(self) -> ir.StructValue:
        """Convert to an ibis struct.

        Returns
        -------
        An ibis struct<full:string, user:string, domain: domain>
        """
        return ibis.struct(
            {
                "full": self.full,
                "user": self.user,
                "domain": self.domain,
            }
        )


class EmailMatchLevel(MatchLevel):
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

    def norm_and_parse(e):
        return ParsedEmail(clean_email(e, normalize=True))

    if isinstance(e1, ir.StringValue):
        e1 = norm_and_parse(e1)
    if isinstance(e2, ir.StringValue):
        e2 = norm_and_parse(e2)

    def f(level: MatchLevel):
        if native_representation == "string":
            return level.as_string()
        else:
            return level.as_integer()

    raw = cases(
        (e1.full == e2.full, f(EmailMatchLevel.FULL_EXACT)),
        (damerau_levenshtein(e1.full, e2.full) <= 1, f(EmailMatchLevel.FULL_NEAR)),
        (e1.user == e2.user, f(EmailMatchLevel.USER_EXACT)),
        (damerau_levenshtein(e1.user, e2.user) <= 1, f(EmailMatchLevel.USER_NEAR)),
        else_=f(EmailMatchLevel.ELSE),
    )
    return EmailMatchLevel(raw)


class EmailsDimension:
    """A dimension of email addresses.

    This is useful if each record contains a collection of email addresses.
    Two records are probably the same if they have a lot of email addresses in common.
    """

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

    def prepare_for_fast_linking(self, t: ir.Table) -> ir.Table:
        """Add a column with the parsed and normalized email addresses."""
        return t.mutate(
            get_column(t, self.column)
            .map(
                lambda email: ParsedEmail(
                    clean_email(email, normalize=True)
                ).as_struct()
            )
            .name(self.column_parsed)
        )

    def prepare_for_blocking(self, t: ir.Table) -> ir.Table:
        return t

    def block(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        linker = UnnestLinker(ibis._[self.column_parsed].full.unnest())
        return linker(t1, t2, **kwargs)

    def compare(self, t: ir.Table) -> ir.Table:
        """Add a column with the best match between all pairs of email addresses."""
        le = t[self.column_parsed + "_l"]
        ri = t[self.column_parsed + "_r"]
        pairs = array_combinations(le, ri)
        min_level = array_min(
            pairs.map(lambda pair: match_level(pair.l, pair.r).as_integer())
        ).fill_null(EmailMatchLevel.ELSE.as_integer())
        return t.mutate(min_level.name(self.column_compared))
