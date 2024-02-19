from __future__ import annotations

from typing import Callable, Iterable, Literal

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo.compare import LevelComparer, compare
from mismo.lib.name._nicknames import are_aliases


class NameLevelComparer:
    """An opinionated [LevelComparer][mismo.compare.LevelComparer] for Human Names.

    This labels record pairs with a level of similarity based on how well
    their names match.
    """

    def __init__(
        self,
        column_left: str | Deferred | Callable[[it.Table], it.StructColumn],
        column_right: str | Deferred | Callable[[it.Table], it.StructColumn],
        levels: list[
            dict[str, str | Deferred | Callable[[it.Table], it.BooleanValue]]
        ] = None,
    ):
        """Compare two tables on the specified name columns.

        A name column is expected to be a Struct of the type
        `struct<
            prefix: string,
            first: string,
            middle: string,
            last: string,
            suffix: string,
            nickname: string,
        >`.

        Parameters
        ----------
        column_left:
            The column in the left table containing the name struct.
        column_right:
            The column in the right table containing the name struct.
        """
        self.column_left = column_left
        self.column_right = column_right
        self.levels = levels or self.default_levels(column_left, column_right)

    @staticmethod
    def default_levels(left: it.StructColumn, right: it.StructColumn):
        return (
            (
                "null",
                lambda t: ibis.or_(
                    is_null(t[left], how="any", fields=["first", "last"]),
                    is_null(t[right], how="any", fields=["first", "last"]),
                ),
            ),
            ("exact", lambda t: exact_match(t[left], t[right])),
            (
                "first_last",
                lambda t: exact_match(t[left], t[right], fields=["first", "last"]),
            ),
            ("nicknames", lambda t: are_match_with_nicknames(t[left], t[right])),
        )

    def __call__(self, t: it.Table) -> it.BooleanColumn:
        return compare(t, LevelComparer("name", self.levels))


def exact_match(
    left: it.StructValue, right: it.StructValue, *, fields: Iterable[str] | None = None
) -> it.BooleanValue:
    """
    The specified fields match exactly. If fields is None, all fields are compared.
    """
    if fields is None:
        return left == right
    return ibis.and_(*(left[f] == right[f] for f in fields))


def is_null(
    struct: it.StructValue, *, how: Literal["any", "all"], fields: Iterable[str] | None
) -> it.BooleanValue:
    """Are any/all of the specified fields in the struct null.

    If fields is None, all fields are compared."""
    if fields is None:
        fields = struct.type().names
    vals = [struct[f].isnull() for f in fields]
    if how == "any":
        return struct.isnull() | ibis.or_(*vals)
    elif how == "all":
        return struct.isnull() | ibis.and_(*vals)
    else:
        raise ValueError(f"how must be 'any' or 'all'. Got {how}")


def are_match_with_nicknames(
    left: it.StructValue, right: it.StructValue
) -> it.BooleanValue:
    """The first names match via nickname or alias, and the last names match."""
    return ibis.and_(
        are_aliases(left["first"], right["first"]),
        left["last"] == right["last"],
    )


def initials_match(left: it.StringValue, right: it.StringValue) -> it.BooleanValue:
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
