from __future__ import annotations

import re
import warnings

import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Expr, Table

from mismo._util import join

# Based on all the JOIN types in
# https://github.com/duckdb/duckdb/blob/b0b1562e293718ee9279c9621cefe4cb5dc01ef9/src/common/enums/physical_operator_type.cpp#L56
# (very good) explanation of these at https://duckdb.org/2022/05/27/iejoin.html
JOIN_TYPES = [
    "LEFT_DELIM_JOIN",
    "RIGHT_DELIM_JOIN",
    "BLOCKWISE_NL_JOIN",
    "NESTED_LOOP_JOIN",
    "HASH_JOIN",
    "PIECEWISE_MERGE_JOIN",
    "IE_JOIN",
    "ASOF_JOIN",
    "CROSS_PRODUCT",
    "POSITIONAL_JOIN",
]

SLOW_JOIN_TYPES = {
    "NESTED_LOOP_JOIN",
    "BLOCKWISE_NL_JOIN",
    "CROSS_PRODUCT",
}


class SlowJoinWarning(UserWarning):
    """Warning for slow join types."""

    def __init__(self, condition, join_type):
        self.condition = condition
        self.join_type = join_type
        try:
            cond_name = str(condition)
        except Exception:
            cond_name = (
                condition.get_name() if isinstance(condition, Expr) else condition
            )
        super().__init__(
            f"The join '{cond_name}' is of type {join_type} and likely to be slow."
        )


class _UnsupportedBackendError(ValueError):
    ...


def warn_if_slow_join(left: Table, right: Table, condition) -> None:
    """Warn if the join in the expression is likely to be slow.

    This is done by checking the join type in the query plan.
    """
    j = join(left, right, condition)
    try:
        ex = _explain_str(j)
    except _UnsupportedBackendError:
        return
    join_type = _extract_top_join_type(ex)
    if join_type in SLOW_JOIN_TYPES:
        warnings.warn(SlowJoinWarning(condition, join_type), stacklevel=2)


# TODO: this appears to be causing flaky test failures, something
# to do with the connection not getting closed in the call to `con.raw_sql`
# E   sqlalchemy.exc.OperationalError: (duckdb.duckdb.TransactionException) TransactionContext Error: cannot start a transaction within a transaction  # noqa: E501
# E   (Background on this error at: https://sqlalche.me/e/20/e3q8)
def _explain_str(duckdb_expr: Expr) -> str:
    # we can't use a separate backend eg from ibis.duckdb.connect()
    # or it might not be able to find the tables/data referenced
    try:
        con = duckdb_expr._find_backend()
    except AttributeError:
        raise _UnsupportedBackendError
    if not isinstance(con, DuckDBBackend):
        raise _UnsupportedBackendError
    sql = ibis.to_sql(duckdb_expr, dialect="duckdb")
    explain_sql = "EXPLAIN " + sql
    with con.raw_sql(explain_sql) as cursor:
        return cursor.fetchall()[0][1]


def _extract_top_join_type(explain_str: str) -> str:
    """Given the output of `EXPLAIN`, return one of the JOIN_TYPES.

    If there are multiple joins in the query, this will return the top (outermost) one.
    """
    # ┌───────────────────────────┐
    # │     BLOCKWISE_NL_JOIN     │
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
    # │           INNER           ├───────────────────────────────────────────┐
    # │       ((a + b) < 9)       │                                           │
    # └─────────────┬─────────────┘                                           │
    # ┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐ # noqa: E501
    # │         PROJECTION        │                             │         PROJECTION        │ # noqa: E501
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │ # noqa: E501
    # │             a             │                             │             b             │ # noqa: E501
    # └─────────────┬─────────────┘                             └─────────────┬─────────────┘ # noqa: E501
    # ┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐ # noqa: E501
    # │         HASH_JOIN         │                             │         SEQ_SCAN          │ # noqa: E501
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │                             │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │ # noqa: E501
    # │           INNER           │                             │             t2            │ # noqa: E501
    # │           a = a           ├──────────────┐              │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │ # noqa: E501
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │              │              │             b             │ # noqa: E501
    # │           EC: 25          │              │              │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │ # noqa: E501
    # │                           │              │              │           EC: 5           │ # noqa: E501
    # └─────────────┬─────────────┘              │              └───────────────────────────┘ # noqa: E501
    # ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
    # │         PROJECTION        ││         PROJECTION        │
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
    # │             a             ││             a             │
    # └─────────────┬─────────────┘└─────────────┬─────────────┘
    # ...
    # │   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   ││   ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─   │
    # │           EC: 5           ││           EC: 5           │
    # └───────────────────────────┘└───────────────────────────┘
    pattern = r"\s+({})\s+".format("|".join(JOIN_TYPES))
    match = re.search(pattern, explain_str)
    if match is None:
        raise ValueError(
            f"Could not find a join type in the explain string: {explain_str}"
        )
    return match.group(1)
