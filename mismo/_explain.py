from __future__ import annotations

import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend

from mismo.exceptions import UnsupportedBackendError


def explain(duckdb_expr: ibis.Expr | str, *, analyze: bool = False) -> str:
    """Get an ASCII art representation of the query plan for a given expression.

    This requires a DuckDB backend.
    """
    # we can't use a separate backend eg from ibis.duckdb.connect()
    # or it might not be able to find the tables/data referenced
    sql, con = _to_sql_and_backend(duckdb_expr)
    if analyze:
        sql = "EXPLAIN ANALYZE " + sql
    else:
        sql = "EXPLAIN " + sql
    cursor = con.raw_sql(sql)
    return cursor.fetchall()[0][1]


def _to_sql_and_backend(duckdb_expr: ibis.Expr | str) -> tuple[str, DuckDBBackend]:
    if isinstance(duckdb_expr, str):
        sql = duckdb_expr
        con = ibis.duckdb.connect()
    else:
        try:
            con = duckdb_expr._find_backend(use_default=True)
        except AttributeError:
            raise NotImplementedError("The given expression must have a backend.")
        if not isinstance(con, DuckDBBackend):
            raise UnsupportedBackendError(con)
        sql = ibis.to_sql(duckdb_expr, dialect="duckdb")
    return sql, con
