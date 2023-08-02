from __future__ import annotations

from functools import cached_property

from ibis import _
from ibis.expr.types import Table

from mismo import _util


class Factorizer:
    """Encode a column as integers.

    Use this class if you have some non-integer column, but you want to use
    integer codes to do some operation, and then restore the original column.
    """

    def __init__(self, t: Table, column: str) -> None:
        """Create a Factorizer from column of values.

        If the input column is already an integer, the Factorizer is a no-op.

        Parameters
        ----------
        t : Table
            The table containing the column to encode.
        column : str
            The name of the column to encode.
        """
        self.t = t
        self.column = column
        self._int_column = _util.unique_column_name(t)
        # The input column is already an integer, so the mapping
        # is simply the identity function. This is an optimization.
        self._noop = t[column].type().is_integer()

    def encode(
        self,
        t: Table | None = None,
        src: str | None = None,
        dst: str | None = None,
        verify: bool = True,
    ) -> Table:
        """Encode a column to integer codes.

        Parameters
        ----------
        t : Table, optional
            The table to encode. If None, use the table passed to the constructor.
        src : str, optional
            The name of the  column to encode.
            If None, use the column passed to the constructor.
        dst : str, optional
            The name of the column to create. If None, overwrite the `src` column.
        verify : bool, default True
            If True, raise an error if the column contains values that are not
            in the original column.
        """
        if t is None:
            t = self.t
        if src is None:
            src = self.column
        if dst is None:
            dst = src

        if self._noop:
            return t.mutate(**{dst: _[src]})

        if t[src] is self.t[self.column]:
            return self._augmented.mutate(**{dst: _[self._int_column]}).drop(
                self._int_column
            )

        if verify:
            if (
                self._mapping_count == 0
                and t.count().execute() > 0
                and t[src].notin(self._mapping.original).any().execute()
            ):
                raise ValueError(
                    f"Column {src} contains values that are not in the original column"
                )

        assert self._int_column not in t.columns
        joined = _util.join(
            t, self._mapping, t[src] == self._mapping.original, how="left"
        )
        if src == "original":
            joined = joined.drop("original_right")
        joined = joined.drop("original")
        result = joined.mutate(**{dst: _[self._int_column]}).drop(self._int_column)
        return result

    def decode(
        self, t: Table, src: str, dst: str | None = None, verify: bool = True
    ) -> Table:
        """Decode a column of integer codes back to the original values.

        Parameters
        ----------
        t : Table
            The table to decode.
        src : str
            The name of the column to decode.
        dst : str, optional
            The name of the column to create. If None, overwrite the `src` column.
        verify : bool, default True
            If True, raise an error if the column contains codes that are not
            froms the original column.
        """
        if dst is None:
            dst = src
        if self._noop:
            return t.mutate(**{dst: _[src]})

        if verify:
            if (
                self._mapping_count == 0
                and t.count().execute() > 0
                and t[src].notin(self._mapping[self._int_column]).any().execute()
            ):
                raise ValueError(
                    f"Column {src} contains codes that are not from the original column"
                )

        assert self._int_column not in t.columns
        joined = _util.join(
            t, self._mapping, t[src] == self._mapping[self._int_column], how="left"
        )
        orig = "original" if src != "original" else "original_right"
        result = joined.mutate(**{dst: _[orig]}).drop(orig, self._int_column)
        return result

    @cached_property
    def _augmented(self) -> Table:
        return self.t.mutate(_util.group_id(self.t, self.column).name(self._int_column))

    @cached_property
    def _mapping(self) -> Table:
        return self._augmented.select(
            self._int_column, original=_[self.column]
        ).distinct()

    @cached_property
    def _mapping_count(self) -> int:
        return self._mapping.count().execute()
