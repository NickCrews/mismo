from __future__ import annotations

from typing import Iterable, Literal

from ibis import _
from ibis.expr.types import Table

from mismo import _join


def join(
    left: Table,
    right: Table,
    condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    dedupe: bool | None = None,
) -> Table:
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
    on_slow:
        What to do if the join condition causes a slow O(n*m) join algorithm.
        If "error", raise a SlowJoinError. If "warn", issue a SlowJoinWarning.
        If "ignore", do nothing.
        See [check_join_type()][mismo.block.check_join_type] for more information.
    dedupe:
        If True, the blocking is assumed to be for a deduplication task, and
        the resulting pairs will have the additional restriction that
        `record_id_l < record_id_r`.
        If False, the resulting pairs will only have the restriction that
        `record_id_l != record_id_r`.
        If None, the restriction will be added if and only if `left` and `right`
        are the same table.
    """
    j = _do_join(left, right, condition, on_slow=on_slow, dedupe=dedupe)
    j = _ensure_suffixed(left.columns, right.columns, j)
    return _order_blocked_data_columns(j)


def _do_join(
    left: Table,
    right: Table,
    condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    dedupe: bool | None,
) -> Table:
    from mismo.block import _sql_analyze

    if id(left) == id(right):
        right = right.view()
        if dedupe is None:
            dedupe = True
    resolved = _join.resolve_predicates(left, right, condition, dedupe=dedupe)
    if len(resolved) == 1 and isinstance(resolved[0], Table):
        return resolved[0]

    if "record_id" in left.columns and "record_id" in right.columns:
        if dedupe:
            resolved = resolved + [left.record_id < right.record_id]
        else:
            resolved = resolved + [left.record_id != right.record_id]

    _sql_analyze.check_join_type(left, right, resolved, on_slow=on_slow)
    result = _join.join(left, right, resolved, lname="{name}_l", rname="{name}_r")
    result = result.distinct()
    return result


def _order_blocked_data_columns(t: Table) -> Table:
    if "record_id_l" not in t.columns or "record_id_r" not in t.columns:
        return t
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]


def _ensure_suffixed(
    original_left_cols: Iterable[str], original_right_cols: Iterable[str], t: Table
) -> Table:
    """Ensure that all columns in `t` have a "_l" or "_r" suffix."""
    lc = set(original_left_cols)
    rc = set(original_right_cols)
    just_left = lc - rc
    just_right = rc - lc
    m = {c + "_l": c for c in just_left} | {c + "_r": c for c in just_right}
    t = t.rename(m)

    # If the condition is an equality condition, like `left.name == right.name`,
    # then since we are doing an inner join ibis doesn't add suffixes to these
    # columns. So we need duplicate these columns and add suffixes.
    un_suffixed = [
        c for c in t.columns if not c.endswith("_l") and not c.endswith("_r")
    ]
    m = {c + "_l": _[c] for c in un_suffixed} | {c + "_r": _[c] for c in un_suffixed}
    t = t.mutate(**m).drop(*un_suffixed)
    return t
