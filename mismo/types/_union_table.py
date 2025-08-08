from __future__ import annotations

from collections.abc import Iterable

import ibis

from mismo.types._table_wrapper import TableWrapper


class UnionTable(TableWrapper):
    """
    A Table whose rows are the non-unique union of the rows from all sub-Tables.
    """

    def __init__(self, tables: Iterable[ibis.Table]) -> None:
        """
        Create a UnionTable from an iterable of ibis Tables.
        """
        tables = tuple(tables)
        if not tables:
            raise ValueError("At least one table must be provided.")
        unioned_table = ibis.union(*tables, distinct=False)
        super().__init__(unioned_table)
        object.__setattr__(self, "_tables", tables)

    @property
    def tables(self) -> tuple[ibis.Table, ...]:
        """
        The tuple of underlying ibis Tables.
        """
        return self._tables

    # skip type hints to inherit from the grandparent ibis.Table
    def filter(self, *predicates):
        return UnionTable(t.filter(*predicates) for t in self.tables)

    # skip type hints to inherit from the grandparent ibis.Table
    def select(self, *exprs, **named_exprs):
        return UnionTable(t.select(*exprs, **named_exprs) for t in self.tables)

    # skip type hints to inherit from the grandparent ibis.Table
    def mutate(self, *args, **kwargs):
        return UnionTable(t.mutate(*args, **kwargs) for t in self.tables)

    # skip type hints to inherit from the grandparent ibis.Table
    def rename(self, method=None, /, **substitutions):
        return UnionTable(t.rename(method, **substitutions) for t in self.tables)

    # skip type hints to inherit from the grandparent ibis.Table
    def drop(self, *fields):
        return UnionTable(t.drop(*fields) for t in self.tables)
