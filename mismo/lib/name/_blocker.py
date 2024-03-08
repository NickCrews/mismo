from __future__ import annotations

from typing import Callable

from ibis import _
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo import _util


class NameBlocker:
    def __init__(
        self,
        *,
        column: str | Deferred | Callable[[it.Table], it.StructColumn] | None = None,
        column_left: str
        | Deferred
        | Callable[[it.Table], it.StructColumn]
        | None = None,
        column_right: str
        | Deferred
        | Callable[[it.Table], it.StructColumn]
        | None = None,
    ):
        """Block two tables on the specified name columns.

        A name column is expected to be a Struct of the type
        `struct<
            prefix: string,
            first: string,
            middle: string,
            last: string,
            suffix: string,
            nickname: string,
        >`.

        Either `column`, or both `column_left` and `column_right` must be specified.

        Parameters
        ----------
        column : str or Deferred or Callable[[Table], StructColumn], optional
            The column in both tables containing the name struct.
        column_left : str or Deferred or Callable[[Table], StructColumn], optional
            The column in the left table containing the name struct.
        column_right : str or Deferred or Callable[[Table], StructColumn], optional
            The column in the right table containing the name struct.
        """
        if column is not None:
            if column_left is not None or column_right is not None:
                raise ValueError(
                    "You must specify either `column` or `column_left` and `column_right`, not both."  # noqa: E501
                )
            self.column_left = column
            self.column_right = column
        else:
            if column_left is None or column_right is None:
                raise ValueError(
                    "You must specify either `column` or `column_left` and `column_right`, not both."  # noqa: E501
                )
            self.column_left = column_left
            self.column_right = column_right

    def __call__(
        self, left: it.Table, right: it.Table, **kwargs
    ) -> tuple[it.Table, it.Table]:
        nl: it.StructColumn = _util.get_column(_, self.column_left)
        tokensl = _oneline(nl).upper().re_split(r"\s+")
        return tokensl.unnest()


def _oneline(name: it.StructValue) -> it.StringValue:
    return (
        name["prefix"].fillna("")
        + " "
        + name["first"].fillna("")
        + " "
        + name["middle"].fillna("")
        + " "
        + name["last"].fillna("")
        + " "
        + name["suffix"].fillna("")
        + " "
        + name["nickname"].fillna("")
    ).strip()
