from __future__ import annotations

from ibis.expr import types as ir

from ._clean import name_tokens, normalize_name


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
    ) -> None:
        self.column = column
        self.column_normed = column_normed.format(column=column)
        self.column_tokens = column_tokens.format(column=column)

    def prep(self, t: ir.Table) -> ir.Table:
        """Normalize and featurize the name column.

        Parameters
        ----------
        t : ir.Table
            The table to prep.

        Returns
        -------
        t : ir.Table
            The prepped table.
        """
        t = t.mutate(normalize_name(t[self.column]).name(self.column_normed))
        t = t.mutate(name_tokens(t[self.column_normed]).name(self.column_tokens))
        return t
