from __future__ import annotations

from contextlib import closing

import ibis
from ibis.expr.types import Expr


def get_explain_str(expr: Expr) -> str:
    backend = expr._find_backend()
    sql = ibis.to_sql(expr, dialect="duckdb")
    explain_sql = "EXPLAIN " + sql
    with closing(backend.raw_sql(explain_sql)) as results:
        return results.fetchall()[0][1]
