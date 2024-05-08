from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util

# Something that can be used to reference a column in a table
_ColumnReferenceLike = Union[
    str,
    ibis.Deferred,
    Callable[[ir.Table], ir.Column],
]
# Something that can be used as a condition in a join between two tables
_ConditionAtom = Union[
    ir.BooleanValue,
    Literal[True],
    tuple[_ColumnReferenceLike, _ColumnReferenceLike],
]
_Condition = Union[
    _ConditionAtom,
    Callable[[ir.Table, ir.Table], _ConditionAtom],
]


def join(
    left: ir.Table,
    right: ir.Table,
    condition: _Condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> ir.Table:
    """Join two tables together using the given condition.

    This is a wrapper around `ibis.join` that

    - Allows for slightly more flexible join conditions.
    - Renames the columns in the resulting table to have "_l" and "_r" suffixes,
      to be consistent with Mismo's conventions.
    - Sorts the output columns into a nicer order.
    - Checks if the join condition is likely to cause a slow O(n*m) join algorithm.
    - If `task` is "dedupe", adds an additional restriction that
      `record_id_l < record_id_r` to remove duplicates.

    Parameters
    ----------
    left
        The left table to block
    right
        The right table to block
    condition
        The condition that determine if two records should be blocked together.

        `conditions` can be any of the following:

        - A string, which is interpreted as the name of a column in both tables.
          eg "price" is equivalent to `left.price == right.price`
        - A Deferred, which is used to reference a column in a table
          eg "_.price.fillna(0)" is equivalent to `left.price.fillna(0) == right.price.fillna(0)`
        - An iterable of the above, which is interpreted as a tuple of conditions.
          eg `("age", _.first_name.upper()")` is equivalent to
          `(left.age == right.age) & (left.first_name.upper() == right.first_name.upper())`
        - A literal `True`, which results in a cross join.
        - A literal `False`, which results in an empty table.
        - A Table in the expected output schema, which is assumed to be
          the result of blocking, and will be used as-is.
        - A callable with the signature
            def block(
                left: Table,
                right: Table,
                *,
                on_slow: Literal["error", "warn", "ignore"] = "error",
                dedupe: bool | None = None,
                **kwargs,
            ) -> BooleanColumn of the join condition, or one of the above.

        !!! note
            You can't reference the input tables directly in the conditions.
            eg `block_one(left, right, left.name == right.name)` will raise an error.
            This is because mismo might be modifying the tables before the
            actual join takes place, which would lead to the condition referencing
            stale tables that don't exist anymore.
            Instead, use a lambda or Deferreds.
    on_slow
        What to do if the join condition causes a slow O(n*m) join algorithm.
        If "error", raise a SlowJoinError.
        If "warn", issue a SlowJoinWarning.
        If "ignore", do nothing.
        See [check_join_algorithm()][mismo.block.check_join_algorithm] for more information.
    task
        If "dedupe", the resulting pairs will have the additional restriction that
        `record_id_l < record_id_r`.
        If "link", no additional restriction is added.
        If None, will be assumed to be "dedupe" if `left` and `right`
        are the same table.
    """  # noqa: E501
    from mismo.block import _sql_analyze

    # if you do block(t.drop("col"), t.drop("col")), you probably want to be doing
    # deduplication. So if tables are *structurally* the same, assume dedupe.
    if id(left) == id(right) or left.equals(right):
        right = right.view()
        if task is None:
            task = "dedupe"
    resolved = _resolve_predicate(
        left, right, condition, on_slow=on_slow, task=task, **kwargs
    )
    if isinstance(resolved, ir.Table):
        return resolved
    left, right, pred = resolved
    if (
        task == "dedupe"
        and "record_id" in left.columns
        and "record_id" in right.columns
    ):
        pred = pred & (left.record_id < right.record_id)

    _sql_analyze.check_join_algorithm(left, right, pred, on_slow=on_slow)
    j = ibis.join(left, right, pred, lname="{name}_l", rname="{name}_r")
    j = _ensure_suffixed(left.columns, right.columns, j)
    j = fix_blocked_column_order(j)
    return j


def block_on_id_pairs(left: ir.Table, right: ir.Table, id_pairs: ir.Table) -> ir.Table:
    left = left.rename("{name}_l")
    right = right.rename("{name}_r")
    if "record_id_l" not in id_pairs.columns or "record_id_r" not in id_pairs.columns:
        raise ValueError(
            "id_pairs must have columns 'record_id_l' and 'record_id_r'.",
            f" Found columns: {id_pairs.columns}",
        )
    j = id_pairs
    j = j.inner_join(right, "record_id_r")
    j = j.inner_join(left, "record_id_l")
    j = fix_blocked_column_order(j)
    return j


def fix_blocked_column_order(t: ir.Table) -> ir.Table:
    """Ensure that the columns in the blocked table are in the expected order."""
    if "record_id_l" not in t.columns or "record_id_r" not in t.columns:
        return t
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]


def _ensure_suffixed(
    original_left_cols: Iterable[str], original_right_cols: Iterable[str], t: ir.Table
) -> ir.Table:
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


def _resolve_predicate(
    left: ir.Table, right: ir.Table, raw, **kwargs
) -> tuple[ir.Table, ir.Table, bool | ir.BooleanColumn] | ir.Table:
    if isinstance(raw, ir.Table):
        return raw
    if isinstance(raw, (ir.BooleanColumn, bool)):
        return left, right, raw
    # Deferred is callable, so guard against that
    if callable(raw) and not isinstance(raw, ibis.Deferred):
        return _resolve_predicate(left, right, raw(left, right, **kwargs))
    keys_l = list(_util.bind(left, raw))
    keys_r = list(_util.bind(right, raw))
    left = left.mutate(keys_l)
    right = right.mutate(keys_r)
    keys_l = [left[val.get_name()] for val in keys_l]
    keys_r = [right[val.get_name()] for val in keys_r]
    cond = ibis.and_(*[lkey == rkey for lkey, rkey in zip(keys_l, keys_r)])
    return left, right, cond
