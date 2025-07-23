from __future__ import annotations

from typing import Callable, Literal

import ibis
from ibis import Deferred, _
from ibis.expr import types as ir

from mismo import linkage, linker


class NameBlocker:
    def __init__(
        self,
        *,
        column: str | Deferred | Callable[[ibis.Table], ir.StructColumn] | None = None,
        column_left: str
        | Deferred
        | Callable[[ibis.Table], ir.StructColumn]
        | None = None,
        column_right: str
        | Deferred
        | Callable[[ibis.Table], ir.StructColumn]
        | None = None,
        max_pairs: int | None = 100_000,
        task: Literal["dedupe", "link"] | None = None,
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
        self.max_pairs = max_pairs
        self.task = task

    def name_keys(self) -> tuple[tuple[ibis.Deferred, ibis.Deferred]]:
        parts = [
            "prefix",
            "given",
            "middle",
            "surname",
            "suffix",
            "nickname",
        ]

        def norm(s):
            return s.upper().strip()

        keys = []
        for pl in parts:
            for pr in parts:
                key_left = norm(_[self.column_left][pl])
                key_right = norm(_[self.column_right][pr])
                keys.append((key_left, key_right))
        return keys

    def __call__(self, left: ibis.Table, right: ibis.Table) -> linkage.Linkage:
        keys = self.name_keys()
        link_tables = [
            linker.KeyLinker(
                (key_left, key_right),
                max_pairs=self.max_pairs,
                task=self.task,
            )(left, right).links.select("record_id_l", "record_id_r")
            for key_left, key_right in keys
        ]
        links_union = ibis.union(*link_tables, distinct=True)
        return linkage.Linkage(
            left=left,
            right=right,
            links=links_union,
        )
