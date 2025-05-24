from __future__ import annotations

from ibis.expr import types as ir

from mismo.lib.name import _clean, _compare


class NameDimension:
    """Prepares, blocks, and compares based on a human name.

    A name is a Struct of the type
    `struct<
        prefix: string,
        given: string,
        middle: string,
        surname: string,
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
        self.comparer = _compare.NameComparer(
            self.column_normed + "_l",
            self.column_normed + "_r",
            result_column=self.column_compared,
        )

    def prepare_for_fast_linking(self, t: ir.Table) -> ir.Table:
        """Add columns with the normalized name.

        Parameters
        ----------
        t
            The table to prep.

        Returns
        -------
        t
            The prepped table.
        """
        t = t.mutate(_clean.normalize_name(t[self.column]).name(self.column_normed))
        return t

    def prepare_for_blocking(self, t: ir.Table) -> ir.Table:
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
        return t.mutate(self.comparer(t))
