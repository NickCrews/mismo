from __future__ import annotations

import re
from typing import Literal
import warnings

import ibis
from ibis.expr import types as ir

from mismo import _explain
from mismo.exceptions import SlowJoinError, SlowJoinWarning, UnsupportedBackendError

JoinAlgorithm = Literal[
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
]

JOIN_ALGORITHMS = frozenset(JoinAlgorithm.__args__)
"""Based on all the JOIN operators in
[https://github.com/duckdb/duckdb/blob/b0b1562e293718ee9279c9621cefe4cb5dc01ef9/src/common/enums/physical_operator_type.cpp#L56]()
(very good) explanation of these at [https://duckdb.org/2022/05/27/iejoin.html]()
"""

SlowJoinAlgorithm = Literal[
    "NESTED_LOOP_JOIN",
    "BLOCKWISE_NL_JOIN",
    "CROSS_PRODUCT",
]

SLOW_JOIN_ALGORITHMS = frozenset(SlowJoinAlgorithm.__args__)


def get_join_algorithm(left: ir.Table, right: ir.Table, condition) -> str:
    """Return one of the JOIN_ALGORITHMS for the outermost join in the expression.

    If there are multiple joins in the query, this will return the top (outermost) one.
    This only works with expressions bound to a DuckDB backend.
    Other kinds of expressions will raise NotImplementedError.
    """
    j = ibis.join(left, right, condition)
    ex = _explain.explain(j)
    return _extract_top_join_alg(ex)


def check_join_algorithm(
    left: ir.Table,
    right: ir.Table,
    condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
) -> None:
    """Error or warn if the join in the expression is likely to be slow.

    Issues a SlowJoinWarning or raises a SlowJoinError.

    This is only implemented for the duckdb backend. All other backends will
    issue a warning and skip the check.

    By "slow", we mean that the join algorithm is one of:

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
    query plan, and checking the join algorithm.

    See [https://duckdb.org/2022/05/27/iejoin.html]() for a very good explanation
    of these join algorithms.
    """
    if on_slow == "ignore":
        return
    try:
        alg = get_join_algorithm(left, right, condition)
    except UnsupportedBackendError as e:
        warnings.warn(
            "We can only check the join algorithm for DuckDB backends. You passed"
            f" an expression with a {type(e.args[0])} backend."
            " To continue using a non-DuckDB backend, you should pass"
            " `on_slow='ignore'` to acknowledge that the join algorithm"
            " won't be checked."
        )
        return
    if alg in SLOW_JOIN_ALGORITHMS:
        if on_slow == "error":
            raise SlowJoinError(condition, alg)
        elif on_slow == "warn":
            warnings.warn(SlowJoinWarning(condition, alg), stacklevel=2)


def _extract_top_join_alg(explain_str: str) -> str:
    """Given the output of `EXPLAIN`, return one of the JOIN_ALGORITHMS.

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
    pattern = r"\s+({})\s+".format("|".join(JOIN_ALGORITHMS))
    match = re.search(pattern, explain_str)
    if match is None:
        raise ValueError(
            f"Could not find a join algorithm in the explain string: {explain_str}"
        )
    return match.group(1)
