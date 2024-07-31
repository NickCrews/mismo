from __future__ import annotations

from typing import Callable

from ibis import Deferred, _
from ibis.expr import types as ir

from mismo import _util, block


class NameBlocker:
    def __init__(
        self,
        *,
        column: str | Deferred | Callable[[ir.Table], ir.StructColumn] | None = None,
        column_left: str
        | Deferred
        | Callable[[ir.Table], ir.StructColumn]
        | None = None,
        column_right: str
        | Deferred
        | Callable[[ir.Table], ir.StructColumn]
        | None = None,
    ):
        """Block two tables on the specified name columns.

        A name column is expected to be a Struct of the type
        `struct<
            prefix: string,
            given: string,
            middle: string,
            surname: string,
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
        self, left: ir.Table, right: ir.Table, **kwargs
    ) -> tuple[ir.Table, ir.Table]:
        def predicate(left, right, **_kwargs):
            nl: ir.StructColumn = _util.get_column(_, self.column_left)
            tokensl = _oneline(nl).upper().re_split(r"\s+")
            return tokensl.unnest()

        return block.KeyBlocker(predicate)(left, right, **kwargs)


def _oneline(name: ir.StructValue) -> ir.StringValue:
    return (
        name.prefix.fill_null("")
        + " "
        + name.given.fill_null("")
        + " "
        + name.middle.fill_null("")
        + " "
        + name.surname.fill_null("")
        + " "
        + name.suffix.fill_null("")
        + " "
        + name.nickname.fill_null("")
    ).strip()
