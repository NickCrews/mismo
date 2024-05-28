from __future__ import annotations

from ibis.expr import types as ir

from mismo.lib.name import _clean, _compare


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
        comparer = _compare.NameComparer(
            self.column_normed + "_l",
            self.column_normed + "_r",
            result_column=self.column_compared,
        )
        return comparer(t)
