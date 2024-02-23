from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

import ibis
from ibis import _
from ibis import selectors as s
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo import _util

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
_Condition = Union[
    _ConditionAtom,
    Callable[[it.Table, it.Table], _ConditionAtom],
]


def block_one(
    left: it.Table,
    right: it.Table,
    condition: _Condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> it.Table:
    """Block two tables together using the given condition.

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

    >>> block_one(t, t, "name")
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
    j = join(left, right, condition, on_slow=on_slow, task=task, **kwargs)
    id_pairs = _distinct_record_ids(j)
    return _join_on_id_pairs(left, right, id_pairs)


def block_many(
    left: it.Table,
    right: it.Table,
    conditions: Iterable[_Condition],
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    labels: bool = False,
    **kwargs,
) -> it.Table:
    """Block two tables using each of the given conditions, then union the results.

    Parameters
    ----------
    left
        The left table to block
    right
        The right table to block
    conditions
        The conditions that determine if two records should be blocked together.
        Each condition is used to join the tables together using `block_one`,
        and then the results are unioned together.
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
    labels
        If True, a column of type `array<string>` will be added to the
        resulting table indicating which
        rules caused each record pair to be blocked.
        If False, the resulting table will only contain the columns of left and
        right.

    Examples
    --------
    >>> import ibis
    >>> from mismo.block import block_one
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

    >>> block_one(t, t, "name")
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
    conds = _util.promote_list(conditions)
    if not conds:
        raise ValueError("No conditions provided")

    def blk(rule):
        j = join(left, right, rule, on_slow=on_slow, task=task, **kwargs)
        ids = _distinct_record_ids(j)
        if labels:
            ids = ids.mutate(blocking_rule=rule)
        return ids

    sub_joined = [blk(rule) for rule in conds]
    if labels:
        result = ibis.union(*sub_joined, distinct=False)
        result = result.group_by(~s.c("blocking_rule")).agg(
            blocking_rules=_.blocking_rule.collect()
        )
        result = result.relocate("blocking_rules", after="record_id_r")
    else:
        result = ibis.union(*sub_joined, distinct=True)
    return _join_on_id_pairs(left, right, result)


def join(
    left: it.Table,
    right: it.Table,
    condition: _Condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> it.Table:
    """A lower-level version of `block_one` that doesn't do any deduplication.

    `block_one()` calls this function, and then adds a deduplication step
    to ensure that there is only one row for every (record_id_l, record_id_r) pair.

    So this has the same behavior as `block_one`,
    but without the final deduplication step.
    """
    from mismo.block import _sql_analyze

    if id(left) == id(right):
        right = right.view()
        if task is None:
            task = "dedupe"
    resolved = _resolve_predicate(
        left, right, condition, on_slow=on_slow, task=task, **kwargs
    )
    if isinstance(resolved, it.Table):
        return resolved

    if (
        task == "dedupe"
        and "record_id" in left.columns
        and "record_id" in right.columns
    ):
        resolved = resolved & (left.record_id < right.record_id)

    _sql_analyze.check_join_algorithm(left, right, resolved, on_slow=on_slow)
    j = ibis.join(left, right, resolved, lname="{name}_l", rname="{name}_r")
    j = _ensure_suffixed(left.columns, right.columns, j)
    j = _move_record_id_cols_first(j)
    return j


def _distinct_record_ids(t: it.Table) -> it.Table:
    return t["record_id_l", "record_id_r"].distinct()


def _join_on_id_pairs(left: it.Table, right: it.Table, id_pairs: it.Table) -> it.Table:
    left = left.rename("{name}_l")
    right = right.rename("{name}_r")
    j = id_pairs
    j = j.inner_join(right, "record_id_r")
    j = j.inner_join(left, "record_id_l")
    j = _move_record_id_cols_first(j)
    return j


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


def _resolve_predicate(
    left: it.Table, right: it.Table, raw, **kwargs
) -> bool | it.BooleanColumn | it.Table:
    if isinstance(raw, (it.Table, it.BooleanColumn, bool)):
        return raw
    if isinstance(raw, (Deferred, str)):
        return _util.get_column(left, raw) == _util.get_column(right, raw)
    # This case must come after the Deferred case, because Deferred is callable
    if callable(raw):
        return _resolve_predicate(left, right, raw(left, right, **kwargs))
    cols = _util.promote_list(raw)
    return ibis.and_(
        *(_util.get_column(left, col) == _util.get_column(right, col) for col in cols)
    )
