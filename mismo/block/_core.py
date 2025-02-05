from __future__ import annotations

from typing import Callable, Literal, NamedTuple, Union

import ibis
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
    Literal[True, False],
    tuple[_ColumnReferenceLike, _ColumnReferenceLike],
]
_IntoCondition = Union[
    _ConditionAtom,
    Callable[[ir.Table, ir.Table], _ConditionAtom],
]


def join(
    left: ir.Table,
    right: ir.Table,
    *conditions: _IntoCondition,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> ir.Table:
    """Join two tables together using the given conditions.

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
    conditions
        The conditions that determine if two records should be blocked together.
        All conditions are ANDed together.
        Each condition can be any of the following:

        - A string, which is interpreted as the name of a column in both tables.
          eg "price" is equivalent to `left.price == right.price`
        - A Deferred, which is used to reference a column in a table
          eg "_.price.fill_null(0)" is equivalent to `left.price.fill_null(0) == right.price.fill_null(0)`
        - A Callable of signature (left: Table, right: Table, args) -> one of the above
        - A 2-tuple tuple of one of the above.
          The first element is for the left table, the second is for the right table.
          This is useful when the column names are different in the two tables,
          or require some transformation/normalization.
          For example `("last_name", _.surname.upper()")` is equivalent to
          `left.last_name == right.surname.upper()`
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
        If None, will be inferred by looking at left and right:
        - If `id(left) == id(right) or left.equals(right)`, then "dedupe" is assumed.
        - Otherwise, "link" is assumed.
    """  # noqa: E501
    from mismo.block import _sql_analyze

    # if you do block(t.drop("col"), t.drop("col")), you probably want to be doing
    # deduplication. So if tables are *structurally* the same, assume dedupe.
    if id(left) == id(right) or left.equals(right):
        right = right.view()
        if task is None:
            task = "dedupe"
    resolved = _resolve_conditions(
        left, right, conditions, on_slow=on_slow, task=task, **kwargs
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
    j = _util.join_ensure_named(left, right, pred, lname="{name}_l", rname="{name}_r")
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


class _Condition(NamedTuple):
    left_column: _ColumnReferenceLike | None = None
    right_column: _ColumnReferenceLike | None = None
    literal: Literal[True, False] | None = None
    table: ir.Table | None = None
    conditions_factory: Callable[[ibis.Table, ibis.Table], ir.BooleanValue] | None = (
        None
    )


# TODO: This is a bit of a hot mess. Can we simplify this?
# TODO: use the shared logic in _resolve.py
def _resolve_conditions(
    left: ir.Table, right: ir.Table, raw: tuple[_IntoCondition], **kwargs
) -> ir.Table | tuple[ir.Table, ir.Table, ir.BooleanValue]:
    conditions = [
        _resolve_condition(left, right, condition, **kwargs) for condition in raw
    ]
    tables = [c.table for c in conditions if c.table]
    if tables:
        if len(tables) > 1:
            raise ValueError("Only one table can be returned from the conditions.")
        if len(conditions) > 1:
            raise ValueError("Only one table can be returned from the conditions.")
        return tables[0]

    l_cols = _util.bind(
        left, (c.left_column for c in conditions if c.left_column is not None)
    )
    r_cols = _util.bind(
        right, (c.right_column for c in conditions if c.right_column is not None)
    )
    left = left.mutate(l_cols)
    right = right.mutate(r_cols)
    equality_conditions = [
        left[left_col.get_name()] == right[right_col.get_name()]
        for left_col, right_col in zip(l_cols, r_cols)
    ]
    literals = [c.literal for c in conditions if c.literal is not None]
    function_conditions = [
        c.conditions_factory(left, right, **kwargs)
        for c in conditions
        if c.conditions_factory is not None
    ]
    all_conditions = equality_conditions + literals + function_conditions
    return left, right, ibis.and_(*all_conditions)


def _resolve_condition(
    left: ir.Table, right: ir.Table, condition, **kwargs
) -> _Condition:
    if isinstance(condition, ir.Table):
        return _Condition(table=condition)
    if isinstance(condition, bool):
        return _Condition(literal=condition)
    if isinstance(condition, (str, ibis.Deferred)):
        return _Condition(left_column=condition, right_column=condition)
    # Deferred is callable, so this needs to come after the guard above
    if callable(condition):
        resolved = condition(left, right, **kwargs)
        if isinstance(resolved, ir.BooleanValue):
            return _Condition(conditions_factory=condition)
        return _resolve_condition(left, right, resolved, **kwargs)
    if isinstance(condition, tuple):
        cl, cr = condition
        return _Condition(left_column=cl, right_column=cr)
    else:
        return _Condition(conditions_factory=condition)
