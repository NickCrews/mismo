from __future__ import annotations

from typing import Iterable

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import vaex
from vaex.dataframe import DataFrame
from vaex.expression import Expression


def explode_table(table: pa.Table, column: str) -> pa.Table:
    """Analgous to pandas.DataFrame.explode()"""
    null_filled = pc.fill_null(table[column], [None])
    flattened = pc.list_flatten(null_filled)
    other_columns = list(table.schema.names)
    other_columns.remove(column)
    if len(other_columns) == 0:
        return pa.table({column: flattened})
    else:
        indices = pc.list_parent_indices(null_filled)
        result = table.select(other_columns).take(indices)
        result = result.append_column(
            pa.field(column, table.schema.field(column).type.value_type),
            flattened,
        )
        return result


def _hash_rows_chunk(*columns: pa.Array) -> pa.Array:
    cols = {str(i): c for i, c in enumerate(columns)}
    table = pa.table(cols)
    pdf = pl.from_arrow(table)
    hashed = pdf.hash_rows()
    return hashed.to_arrow()


@vaex.register_dataframe_accessor("mismo", override=True)
class MismoOps:
    def __init__(self, df: DataFrame):
        self.df = df

    def explode(self, column: str) -> DataFrame:
        at = self.df.to_arrow_table()
        exp = explode_table(at, column)
        return vaex.from_arrow_table(exp)

    def hash_rows(self, columns: str | Iterable[str] | None = None) -> Expression:
        if columns is None:
            columns = self.df.column_names
        elif isinstance(columns, str):
            columns = [columns]
        return self.df.apply(_hash_rows_chunk, columns, vectorize=True)


@vaex.register_function(on_expression=True)
def struct_get(column: pa.Array | pa.ChunkedArray, field: str | int) -> pa.StructArray:
    """Workaround for a byg where Expression.struct.get() breaks for chunked arrays"""
    if isinstance(column, pa.Array):
        return column.field(field)
    elif isinstance(column, pa.ChunkedArray):
        return pa.chunked_array([chunk.field(field) for chunk in column.chunks])
    else:
        raise ValueError("Can only get struct from Array or ChunkedArray")
