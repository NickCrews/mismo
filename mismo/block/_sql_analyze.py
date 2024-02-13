from __future__ import annotations

import re
from typing import Literal
import warnings

import ibis
from ibis.backends.duckdb import Backend as DuckDBBackend
from ibis.expr.types import Expr, Table

from mismo import _util
from mismo._join import join

JOIN_TYPES = frozenset(
    {
        # If the duckdb analyzer can see the condition is always false (eg you pass 1=2)
        "EMPTY_RESULT",  # O(1)
        "LEFT_DELIM_JOIN",  # ??
        "RIGHT_DELIM_JOIN",  # ??
        "BLOCKWISE_NL_JOIN",  # O(n*m)
        "NESTED_LOOP_JOIN",  # O(n*m)
        "HASH_JOIN",  # O(n)
        "PIECEWISE_MERGE_JOIN",  # O(m*log(n))
        "IE_JOIN",  # O(n*log(n))
        "ASOF_JOIN",  # ??
        "CROSS_PRODUCT",  # O(n*m)
        "POSITIONAL_JOIN",  # O(n)
    }
)
"""Based on all the JOIN types in
[https://github.com/duckdb/duckdb/blob/b0b1562e293718ee9279c9621cefe4cb5dc01ef9/src/common/enums/physical_operator_type.cpp#L56]()
(very good) explanation of these at [https://duckdb.org/2022/05/27/iejoin.html]()
"""

SLOW_JOIN_TYPES = frozenset(
    {
        "NESTED_LOOP_JOIN",
        "BLOCKWISE_NL_JOIN",
        "CROSS_PRODUCT",
    }
)


class _SlowJoinMixin:
    def __init__(self, condition, join_type: str) -> None:
        self.condition = condition
        self.join_type = join_type
        super().__init__(
            f"The join '{_util.get_name(self.condition)}' is of type {join_type} and likely to be slow."  # noqa: E501
        )


class SlowJoinWarning(_SlowJoinMixin, UserWarning):
    """Warning for slow join types."""


class SlowJoinError(_SlowJoinMixin, ValueError):
    """Error for slow join types."""


def get_join_type(left: Table, right: Table, condition) -> str:
    """Return one of the JOIN_TYPES for the outermost join in the expression.

    If there are multiple joins in the query, this will return the top (outermost) one.
    This only works with expressions bound to a DuckDB backend.
    Other kinds of expressions will raise NotImplementedError.
    """
    j = join(left, right, condition)
    ex = _explain_str(j)
    join_type = _extract_top_join_type(ex)
    return join_type


def check_join_type(
    left: Table,
    right: Table,
    condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
) -> None:
    """Error or warn if the join in the expression is likely to be slow.

    Issues a SlowJoinWarning or raises a SlowJoinError.

    By "slow", we mean that the join is one of:

    - "NESTED_LOOP_JOIN" O(n*m)
    - "BLOCKWISE_NL_JOIN" O(n*m)
    - "CROSS_PRODUCT" O(n*m)

    and not one of the fast join algorithms:

    - "EMPTY_RESULT" O(1)
    - "POSITIONAL_JOIN" O(n)
    - "HASH_JOIN" O(n)
    - "PIECEWISE_MERGE_JOIN" O(m*log(n))
    - "IE_JOIN" O(n*log(n))
    - "ASOF_JOIN" O(n*log(n))

    This is done by using the EXPLAIN command to generate the
    query plan, and checking the join type.

    See [https://duckdb.org/2022/05/27/iejoin.html]() for a very good explanation
    of these join types.
    """
    if on_slow == "ignore":
        return
    join_type = get_join_type(left, right, condition)
    if join_type in SLOW_JOIN_TYPES:
        if on_slow == "error":
            raise SlowJoinError(condition, join_type)
        elif on_slow == "warn":
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
        raise NotImplementedError("The given expression must have a backend.")
    if not isinstance(con, DuckDBBackend):
        raise NotImplementedError("The given expression must be a DuckDB expression.")
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
