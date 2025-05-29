from __future__ import annotations

from collections.abc import Iterable, Mapping
import functools
from typing import Generic, Literal, TypeVar

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util
from mismo.types._table_wrapper import TableWrapper

T = TypeVar("T", bound=ir.Column)


class StatsTable(TableWrapper, Generic[T]):
    value: T
    count: ir.IntegerColumn
    frequency: ir.FloatingColumn

    @classmethod
    def from_column(cls, column: T) -> StatsTable[T]:
        t = column.name("value").as_table()
        n_total = t.count().as_scalar()
        t = t.aggregate(
            count=_.count().cast("uint32"),
            frequency=_.count() / n_total,
            by="value",
        )
        return cls(t)


class ColumnStats:
    """Represents statistics for a column."""

    def __init__(self, column: ir.Column):
        self.column = column
        self._stats_table = StatsTable.from_column(column)

    @property
    def name(self) -> str:
        """The name of the term."""
        return self.column.get_name()

    @functools.cached_property
    def stats_table(self) -> StatsTable:
        """The stats table for this column."""
        return StatsTable(self._stats_table.cache())

    def add_frequencies(
        self,
        table: ibis.Table,
        *,
        column: str | ibis.Deferred | ibis.Column = None,
        name_as: str | None = None,
        default: Literal["1/N"] | int | float = "1/N",
    ) -> ibis.Table:
        """Add frequency columns to the given table."""
        if name_as is None:
            name_as = f"frequency_{self.name}"
        if column is None:
            column = self.name

        if default == "1/N" or default == "1/n":
            n_total = table.count().as_scalar()
            default = (1 / n_total).cast("float64")
        elif isinstance(default, ibis.Scalar):
            default = default.cast("float64")
        else:
            default = ibis.literal(default, "float64")

        table_column = table.bind(column)[0]

        unique_name = _util.unique_name("join_key")
        # TODO: this could be factored out into a join_lookup() function
        stats_raw = self.stats_table.select(
            _.value.name(unique_name), _.frequency.name(name_as)
        )
        filler = (
            table_column.name(unique_name)
            .as_table()
            .distinct()
            .anti_join(stats_raw, unique_name)
            .mutate(default.name(name_as))
        )
        stats = ibis.union(stats_raw, filler)
        assert stats.columns == (unique_name, name_as)
        table = table.left_join(stats, table_column == stats[unique_name]).drop(
            unique_name
        )
        return table


class TermFrequencyModel:
    def __init__(
        self,
        columns: ibis.Table
        | ibis.Column
        | Mapping[str, ibis.Column]
        | Iterable[ibis.Column],
        /,
    ):
        if isinstance(columns, Mapping):
            self.columns = columns
        elif isinstance(columns, ibis.Table):
            self.columns = {col: columns[col] for col in columns.columns}
        elif isinstance(columns, ibis.Column):
            self.columns = {columns.get_name(): columns}
        else:
            self.columns = {col.get_name(): col for col in columns}

    @property
    def column_stats(self) -> dict[str, ColumnStats]:
        return {
            name: ColumnStats(column.name(name))
            for name, column in self.columns.items()
        }

    def add_term_frequencies(
        self,
        table: ibis.Table,
        *,
        columns: dict[str, str | ibis.Deferred | ibis.Column] | None = None,
        name_as: str | None = None,
        default: Literal[0, "1/N"] = "1/N",
    ) -> ibis.Table:
        """Add frequency columns for all terms in the model to the table."""
        if name_as is None:
            name_as = "term_frequencies"
        freq_names = []
        for column_name, term in self.column_stats.items():
            unique_name = _util.unique_name("freq")
            column = None if columns is None else columns.get(column_name)
            table = term.add_frequencies(
                table, column=column, name_as=unique_name, default=default
            )
            freq_names.append(unique_name)
        table = table.mutate(
            _product(*[table[name] for name in freq_names]).name(name_as)
        ).drop(*freq_names)
        return table

    def __repr__(self):
        return f"{self.__class__.__name__}({list(self.columns.keys())})"


def _product(*args: ir.Column) -> ir.Column:
    result = args[0]
    for arg in args[1:]:
        result = result * arg
    return result
