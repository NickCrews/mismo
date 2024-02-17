from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

import ibis
from ibis import _
from ibis import selectors as s
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo import _join, _util

# Something that can be used to reference a column in a table
_ColumnReferenceLike = Union[
    str,
    Deferred,
    Callable[[it.Table], it.Column],
]
# Something that can be used as a condition in a join between two tables
_ConditionAtom = Union[
    it.BooleanValue,
    Literal[True],
    tuple[_ColumnReferenceLike, _ColumnReferenceLike],
]
_ConditionOrConditions = Union[
    _ConditionAtom,
    Iterable[_ConditionAtom],
]
_Condition = Union[
    _ConditionOrConditions,
    Callable[[it.Table, it.Table], _ConditionOrConditions],
]


def block(
    left: it.Table,
    right: it.Table,
    conditions,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    labels: bool = False,
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> it.Table:
    """Block two tables together using the given conditions.

    Parameters
    ----------
    left
        The left table to block
    right
        The right table to block
    conditions
        The conditions that determine if two records should be blocked together.
        Each condition is used to join the tables together, and then the results
        are unioned together.

        `conditions` can be any of the following:

        - A string, which is interpreted as the name of a column in both tables.
          eg "price" is equivalent to `left.price == right.price`
        - A Deferred, which is used to reference a column in a table
          eg "_.price.fillna(0)" is equivalent to `left.price.fillna(0) == right.price.fillna(0)`
        - A tuple of two of the above, which is interpreted as a join condition,
          in case you need to tread the two tables differently.
          eg `("price", _.cost.fillna(0)")` is equivalent to `left.price == right.cost.fillna(0)`
        - A literal `True`, which results in a cross join.
        - A literal `False`, which results in an empty table.
        - A Table in the expected output schema, which is assumed to be
          the result of blocking, and will be used as-is.
        - A callable with the signature
            def block(
                left: Table,
                right: Table,
                conditions,
                *,
                on_slow: Literal["error", "warn", "ignore"] = "error",
                labels: bool = False,
                dedupe: bool | None = None,
                **kwargs,
            ) -> <one of the above>

        !!! note
            You can't reference the input tables directly in the conditions.
            eg `block(left, right, left.name == right.name)` will raise an error.
            This is because mismo might be modifying the tables before the
            actual join takes place, which would lead to the condition referencing
            stale tables that don't exist anymore.
            Instead, use a lambda or Deferreds.
    on_slow
        What to do if the join condition causes a slow O(n*m) join algorithm.
        If "error", raise a SlowJoinError. If "warn", issue a SlowJoinWarning.
        If "ignore", do nothing.
        See [check_join_algorithm()][mismo.block.check_join_algorithm] for more information.
    labels
        If True, a column of type `array<string>` will be added to the
        resulting table indicating which
        rules caused each record pair to be blocked.
        If False, the resulting table will only contain the columns of left and
        right.
    task
        If "dedupe", the resulting pairs will have the additional restriction that
        `record_id_l < record_id_r`.
        If None, will be assumed to be "dedupe" if `left` and `right`
        are the same table.

    Examples
    --------
    >>> import ibis
    >>> from mismo.block import block
    >>> from mismo.datasets import load_patents
    >>> ibis.options.interactive = True
    >>> t = load_patents()["record_id", "name", "latitude"]
    >>> t
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
    ┃ record_id ┃ name                         ┃ latitude ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
    │ int64     │ string                       │ float64  │
    ├───────────┼──────────────────────────────┼──────────┤
    │      2909 │ * AGILENT TECHNOLOGIES, INC. │     0.00 │
    │      3574 │ * AKZO NOBEL N.V.            │     0.00 │
    │      3575 │ * AKZO NOBEL NV              │     0.00 │
    │      3779 │ * ALCATEL N.V.               │    52.35 │
    │      3780 │ * ALCATEL N.V.               │    52.35 │
    │      3782 │ * ALCATEL N.V.               │     0.00 │
    │     15041 │ * CANON EUROPA N.V           │     0.00 │
    │     15042 │ * CANON EUROPA N.V.          │     0.00 │
    │     15043 │ * CANON EUROPA NV            │     0.00 │
    │     25387 │ * DSM N.V.                   │     0.00 │
    │         … │ …                            │        … │
    └───────────┴──────────────────────────────┴──────────┘

    Block the table with itself wherever the names match:

    >>> block(t, t, "name")
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                       ┃ name_r                       ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                       │ string                       │
    ├─────────────┼─────────────┼────────────┼────────────┼──────────────────────────────┼──────────────────────────────┤
    │        2909 │        2909 │       0.00 │        0.0 │ * AGILENT TECHNOLOGIES, INC. │ * AGILENT TECHNOLOGIES, INC. │
    │        3574 │        3574 │       0.00 │        0.0 │ * AKZO NOBEL N.V.            │ * AKZO NOBEL N.V.            │
    │        3575 │        3575 │       0.00 │        0.0 │ * AKZO NOBEL NV              │ * AKZO NOBEL NV              │
    │        3779 │        3782 │      52.35 │        0.0 │ * ALCATEL N.V.               │ * ALCATEL N.V.               │
    │        3780 │        3782 │      52.35 │        0.0 │ * ALCATEL N.V.               │ * ALCATEL N.V.               │
    │        3782 │        3782 │       0.00 │        0.0 │ * ALCATEL N.V.               │ * ALCATEL N.V.               │
    │       15041 │       15041 │       0.00 │        0.0 │ * CANON EUROPA N.V           │ * CANON EUROPA N.V           │
    │       15042 │       15042 │       0.00 │        0.0 │ * CANON EUROPA N.V.          │ * CANON EUROPA N.V.          │
    │       15043 │       15043 │       0.00 │        0.0 │ * CANON EUROPA NV            │ * CANON EUROPA NV            │
    │       25388 │     7651594 │       0.00 │        0.0 │ DSM N.V.                     │ DSM N.V.                     │
    │           … │           … │          … │          … │ …                            │ …                            │
    └─────────────┴─────────────┴────────────┴────────────┴──────────────────────────────┴──────────────────────────────┘


    """  # noqa: E501
    conds = _to_conditions(conditions)
    if not conds:
        raise ValueError("No conditions provided")

    sub_joined = [
        _block_one(
            left, right, rule, labels=labels, on_slow=on_slow, task=task, **kwargs
        )
        for rule in conds
    ]
    if labels:
        result = ibis.union(*sub_joined, distinct=False)
        result = result.group_by(~s.c("blocking_rule")).agg(
            blocking_rules=_.blocking_rule.collect()
        )
        result = result.relocate("blocking_rules", after="record_id_r")
    else:
        result = ibis.union(*sub_joined, distinct=True)
    return result


def _block_one(
    left: it.Table,
    right: it.Table,
    condition,
    *,
    labels: bool = False,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> it.Table:
    j = _do_join(left, right, condition, on_slow=on_slow, task=task, **kwargs)
    j = _ensure_suffixed(left.columns, right.columns, j)
    if labels:
        j = j.mutate(blocking_rule=_util.get_name(condition))
    return _move_record_id_cols_first(j)


def _do_join(
    left: it.Table,
    right: it.Table,
    condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None,
    **kwargs,
) -> it.Table:
    from mismo.block import _sql_analyze

    if id(left) == id(right):
        right = right.view()
        if task is None:
            task = "dedupe"
    resolved = _join.resolve_predicates(
        left, right, condition, on_slow=on_slow, task=task, **kwargs
    )
    if len(resolved) == 1 and isinstance(resolved[0], it.Table):
        return resolved[0]

    if (
        task == "dedupe"
        and "record_id" in left.columns
        and "record_id" in right.columns
    ):
        resolved = resolved + [left.record_id < right.record_id]

    _sql_analyze.check_join_algorithm(left, right, resolved, on_slow=on_slow)
    result = _join.join(left, right, resolved, lname="{name}_l", rname="{name}_r")
    result = result.distinct()
    return result


def _move_record_id_cols_first(t: it.Table) -> it.Table:
    if "record_id_l" not in t.columns or "record_id_r" not in t.columns:
        return t
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]


def _ensure_suffixed(
    original_left_cols: Iterable[str], original_right_cols: Iterable[str], t: it.Table
) -> it.Table:
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


def _to_conditions(x) -> list[_Condition]:
    if isinstance(x, tuple) and len(x) == 2:
        return [x]
    return _util.promote_list(x)
