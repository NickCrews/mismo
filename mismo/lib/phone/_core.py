from __future__ import annotations

from typing import Literal, overload

from ibis.expr import types as ir

from mismo._util import cases, get_column
from mismo.arrays import array_combinations, array_min
from mismo.compare import MatchLevel
from mismo.text import damerau_levenshtein


@overload
def clean_phone_number(
    phones: ir.Table, *, default_area_code: str | None = None
) -> ir.Table: ...


@overload
def clean_phone_number(
    phones: ir.ArrayValue, *, default_area_code: str | None = None
) -> ir.ArrayValue: ...


@overload
def clean_phone_number(
    phones: ir.StringValue, *, default_area_code: str | None = None
) -> ir.StringValue: ...


def clean_phone_number(numbers, *, default_area_code=None):
    """Extracts any 10-digit number from a string.

    Drops leading 1 country code if present.

    Parsing failures are returned as NULL.

    Empty strings are returned as NULL.

    If you supply a default_area_code, it will be prepended to 7-digit numbers.

    If a number looks bogus, ie it contains "0000", "9999", or "12345",
    it is set to NULL.
    """

    def f(n):
        return _clean_phone_number(n, default_area_code=default_area_code)

    if isinstance(numbers, ir.StringValue):
        return f(numbers)
    elif isinstance(numbers, ir.ArrayValue):
        return numbers.map(f).filter(lambda x: x.notnull()).unique()
    elif isinstance(numbers, ir.Table):
        return numbers.mutate(
            phones=clean_phone_number(
                numbers.phones, default_area_code=default_area_code
            )
        )
    raise ValueError(f"Unexpected type {type(numbers)}")


def _clean_phone_number(
    numbers: ir.StringValue, *, default_area_code: str | None = None
) -> ir.StringValue:
    x = numbers
    x = x.cast("string")
    x = x.re_replace(r"[^0-9]", "")
    if default_area_code:
        if len(default_area_code) != 3:
            raise ValueError("default_area_code must be 3 digits")
        x = x.re_replace(r"^(\d{7})$", rf"{default_area_code}\1")
    x = x.re_extract(r"1?(\d{10})", 1)
    x = x.nullif("")
    x = _drop_bogus_numbers(x)
    return x


def _drop_bogus_numbers(numbers: ir.StringValue) -> ir.StringValue:
    bogus_substrings = ["0000", "9999", "12345"]
    pattern = "|".join(bogus_substrings)
    is_bogus = numbers.re_search(".*" + pattern + ".*")
    return is_bogus.ifelse(None, numbers)


class PhoneMatchLevel(MatchLevel):
    """How closely two phone numbers match."""

    EXACT = 0
    """The numbers are exactly the same."""
    NEAR = 1
    """The numbers have a small edit distance."""
    ELSE = 2
    """None of the above."""


def match_level(
    p1: ir.StringValue,
    p2: ir.StringValue,
    *,
    native_representation: Literal["integer", "string"] = "integer",
) -> PhoneMatchLevel:
    """Match level of two phone numbers.

    Assumes the phone numbers have already been cleaned and normalized.

    Parameters
    ----------
    p1 :
        The first phone number.
    p2 :
        The second phone number.

    Returns
    -------
    level:
        The match level.
    """

    def f(level: MatchLevel):
        if native_representation == "string":
            return level.as_string()
        else:
            return level.as_integer()

    raw = cases(
        (p1 == p2, f(PhoneMatchLevel.EXACT)),
        (damerau_levenshtein(p1, p2) <= 1, f(PhoneMatchLevel.NEAR)),
        else_=f(PhoneMatchLevel.ELSE),
    )
    return PhoneMatchLevel(raw)


class PhonesDimension:
    """Prepares, blocks, and compares sets of phone numbers.

    This is useful if each record contains a collection of phone numbers.
    Two records are probably the same if they have a lot of phone numbers in common.
    """

    def __init__(
        self,
        column: str,
        *,
        column_cleaned: str = "{column}_cleaned",
        column_compared: str = "{column}_compared",
    ):
        """Initialize the dimension.

        Parameters
        ----------
        column :
            The name of the column that holds a array<string> of phone numbers.
        column_cleaned :
            The name of the column that will be filled with the parsed phone numbers.
        column_compared :
            The name of the column that will be filled with the comparison results.
        """
        self.column = column
        self.column_cleaned = column_cleaned.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prepare_for_fast_linking(self, t: ir.Table) -> ir.Table:
        """Add a column with the parsed and normalized phone numbers."""
        return t.mutate(
            get_column(t, self.column).map(clean_phone_number).name(self.column_cleaned)
        )

    def prepare_for_blocking(self, t: ir.Table) -> ir.Table:
        """noop"""
        return t

    def compare(self, t: ir.Table) -> ir.Table:
        """Add a column with the best match between all pairs of phone numbers."""
        le = t[self.column_cleaned + "_l"]
        ri = t[self.column_cleaned + "_r"]
        pairs = array_combinations(le, ri)
        min_level = array_min(
            pairs.map(lambda pair: match_level(pair.l, pair.r).as_integer())
        ).fill_null(PhoneMatchLevel.ELSE.as_integer())
        return t.mutate(min_level.name(self.column_compared))
