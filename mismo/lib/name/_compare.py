from __future__ import annotations

from typing import Callable

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo.compare import ComparisonLevel
from mismo.lib.name._nicknames import are_aliases


class NameComparer:
    def __init__(
        self,
        column_left: str | Deferred | Callable[[it.Table], it.StructColumn],
        column_right: str | Deferred | Callable[[it.Table], it.StructColumn],
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

    @property
    def levels(self):
        return [
            ComparisonLevel(
                name="exact",
                condition=lambda t: t[self.column_left] == t[self.column_right],
            ),
            ComparisonLevel(
                name="nicknames",
                condition=lambda t: are_aliases(
                    t[self.column_left], t[self.column_right]
                ),
            ),
        ]


def are_match_with_nicknames(
    left: it.StructValue, right: it.StructValue
) -> it.BooleanValue:
    return ibis.and_(
        are_aliases(left.first, right.first),
        left.last == right.last,
    )
