from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ibis.common.deferred import Deferred
from ibis.expr import types as ir
from ibis.expr.types.relations import bind

from mismo import _util


def compare(t: ir.Table, *comparers: Any) -> ir.Table:
    """Apply the supplied comparers in order to the table.

    Parameters
    ----------
    t:
        The table to compare.
    comparers:
        Each one can be:

        - an Ibis Expression
        - a Deferred
        - a Mapping[str, ...] of column names to expressions
        - a Table, in which case it is returned as is
        - a callable that takes a table and returns one of the above

    Returns
    -------
    Table
        A mutated table with the specified comparers applied.
    """
    for c in comparers:
        t = _compare_one(t, c)
    return t


def _compare_one(t: ir.Table, comparer) -> ir.Table:
    if isinstance(comparer, ir.Table):
        return comparer
    if isinstance(comparer, Mapping):
        return t.mutate(**comparer)
    name = _util.get_name(comparer)
    if isinstance(comparer, Deferred):
        return t.mutate(name=bind(t, comparer))
    if isinstance(comparer, ir.Expr):
        return t.mutate(name=comparer)
    results = comparer(t)
    if isinstance(results, ir.Table):
        return results
    results = {name: results}
    return compare(t, results)
