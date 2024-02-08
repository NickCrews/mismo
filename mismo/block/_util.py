from __future__ import annotations

from ibis import _
from ibis.expr.types import Table

from mismo import _util
from mismo.block import _sql_analyze


def join(left: Table, right: Table, condition, *, warn_on_slow: bool = True) -> Table:
    """Join two tables, afterwards adding a "_l" or "_r" suffix to all columns.

    Unlike ibis, which only adds suffixes to columns that are duplicated,
    this *always* adds suffixes to all columns.

    Parameters
    ----------
    left:
        The left table
    right:
        The right table
    condition:
        Anything that [Ibis accepts as a join condition](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.join).
        This should be specified
        without the "_l" and "_r" suffixes, eg `left.name == right.name`, not
        `left.name_l == right.name_r`, because the suffixes will be added
        AFTER the join.
    warn_on_slow:
        If True, raise a SlowJoinWarning if the join is likely to be slow.
        Currently only works with DuckDB backends.

        By "slow", we mean that the join is one of:

        - nested loop join O(n*m)
        - cross product O(n*m)

        and not one of the fast join algorithms:

        - hash join O(n)
        - range join O(n*log(n))
        - merge join O(m*log(n))
        - inequality join O(n*log(n))

        This is done by using the EXPLAIN command to generate the
        query plan, and checking the join type.

        See https://duckdb.org/2022/05/27/iejoin.html for a very good explanation
        of these join types.
    """
    if left is right:
        right = right.view()
    lc = set(left.columns)
    rc = set(right.columns)
    just_left = lc - rc
    just_right = rc - lc
    if warn_on_slow:
        _sql_analyze.warn_if_slow_join(left, right, condition)
    raw = _util.join(left, right, condition, lname="{name}_l", rname="{name}_r")
    left_renaming = {c + "_l": c for c in just_left}
    right_renaming = {c + "_r": c for c in just_right}
    renaming = {**left_renaming, **right_renaming}
    result = raw.rename(renaming)

    # If the condition is an equality condition, like `left.name == right.name`,
    # then since we are doing an inner join ibis doesn't add suffixes to these
    # columns. So we need duplicate these columns and add suffixes.
    un_suffixed = [
        c for c in result.columns if not c.endswith("_l") and not c.endswith("_r")
    ]
    suffix_map = {c + "_l": _[c] for c in un_suffixed} | {
        c + "_r": _[c] for c in un_suffixed
    }
    result = result.mutate(**suffix_map).drop(*un_suffixed)

    return order_blocked_data_columns(result)


def order_blocked_data_columns(t: Table) -> Table:
    if "record_id_l" not in t.columns or "record_id_r" not in t.columns:
        return t
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]
