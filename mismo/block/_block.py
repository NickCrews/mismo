from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

import ibis
from ibis import _
from ibis import selectors as s
from ibis.common.deferred import Deferred
from ibis.expr import types as ir

from mismo import _util

# Something that can be used to reference a column in a table
_ColumnReferenceLike = Union[
    str,
    Deferred,
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


def block_one(
    left: ir.Table,
    right: ir.Table,
    condition: _Condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> ir.Table:
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
    >>> from ibis import _
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> t = mismo.datasets.load_patents()["record_id", "name", "latitude"]
    >>> t.head()
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
    └───────────┴──────────────────────────────┴──────────┘

    Block the table with itself wherever the names match:

    >>> mismo.block.block_one(t, t, "name").head()
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l               ┃ name_r               ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string               │ string               │
    ├─────────────┼─────────────┼────────────┼────────────┼──────────────────────┼──────────────────────┤
    │      665768 │      665769 │  51.683333 │        0.0 │ ALCOA NEDERLAND B.V. │ ALCOA NEDERLAND B.V. │
    │     1598894 │     1598895 │  51.416667 │        0.0 │ ASML NETHERLAND B.V. │ ASML NETHERLAND B.V. │
    │     4332214 │     4332215 │  52.350000 │        0.0 │ Canon Europa N.V.    │ Canon Europa N.V.    │
    │     7651166 │     7651167 │  50.900000 │        0.0 │ DSM B.V.             │ DSM B.V.             │
    │     7651339 │     7651340 │  50.900000 │       50.9 │ DSM I.P. Assets B.V. │ DSM I.P. Assets B.V. │
    └─────────────┴─────────────┴────────────┴────────────┴──────────────────────┴──────────────────────┘

    Arbitrary blocking keys are supported. Example: block the table wherever
    - the first 5 characters of the name in uppercase, are the same
    AND
    - the latitudes, rounded to 1 decimal place, are the same

    >>> mismo.block.block_one(t, t, (_["name"][:5].upper(), _.latitude.round(1))).head()
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                ┃ name_r                     ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                │ string                     │
    ├─────────────┼─────────────┼────────────┼────────────┼───────────────────────┼────────────────────────────┤
    │        3574 │        3575 │   0.000000 │   0.000000 │ * AKZO NOBEL N.V.     │ * AKZO NOBEL NV            │
    │      663246 │      663255 │  52.016667 │  52.025498 │ Alcatel NV            │ ALCATEL N.V., RIJSWIJK, NL │
    │      665768 │      665773 │  51.683333 │  51.683333 │ ALCOA NEDERLAND B.V.  │ Alcoa Nederland B.V.       │
    │     1598972 │     1598988 │  51.416667 │  51.416667 │ Asml Netherlands B.V. │ ASML Netherlands-B.V.      │
    │     7651427 │     7651428 │  50.900000 │  50.900000 │ DSM IP assets B.V.    │ DSM Ip Assets B.V.         │
    └─────────────┴─────────────┴────────────┴────────────┴───────────────────────┴────────────────────────────┘

    We can even block on arrays! For example, first let's split each name into
    significant tokens:

    >>> tokens = _.name.upper().split(" ").filter(lambda x: x.length() > 4)
    >>> tokens.resolve(t)
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ ArrayFilter(StringSplit(Uppercase(name), ' '), Greater(StringLength(x), 4)) ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                                                               │
    ├─────────────────────────────────────────────────────────────────────────────┤
    │ ['AGILENT', 'TECHNOLOGIES,']                                                │
    │ ['NOBEL']                                                                   │
    │ ['NOBEL']                                                                   │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ []                                                                          │
    │ …                                                                           │
    └─────────────────────────────────────────────────────────────────────────────┘

    Now, block the tables together wherever two records share a token.
    Note that this blocked `* SCHLUMBERGER LIMITED` with `* SCHLUMBERGER TECHNOLOGY BV`.

    >>> b = mismo.block.block_one(t, t, tokens.unnest())
    >>> b[_.name_l != _.name_r]
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                                            ┃ name_r                                            ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                                            │ string                                            │
    ├─────────────┼─────────────┼────────────┼────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────────────┤
    │        3574 │        3575 │   0.000000 │   0.000000 │ * AKZO NOBEL N.V.                                 │ * AKZO NOBEL NV                                   │
    │       62445 │       66329 │   0.000000 │   0.000000 │ * N V PHILIPS' GLOEILAMPENFABRIEKEN               │ * N.V. PHILIPS' GLOEILAMPENFABRIEKEN              │
    │       79860 │       79872 │  52.500000 │   0.000000 │ * SCHLUMBERGER LIMITED                            │ * SCHLUMBERGER TECHNOLOGY BV                      │
    │       81613 │       81633 │  52.083333 │  52.083333 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ B.V. │
    │       81631 │       81641 │  52.500000 │  52.083333 │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ B.V. │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ BV   │
    │       81614 │      317966 │   0.000000 │  52.350000 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ Adidas International Marketing B.V.               │
    │       81614 │      317969 │   0.000000 │  52.350000 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ adidas International Marketing B.V,               │
    │      317969 │      317971 │  52.350000 │  52.500000 │ adidas International Marketing B.V,               │ adidas International Marketing B.V.               │
    │      317967 │      317971 │   0.000000 │  52.500000 │ Adidas International Marketing B.V.               │ adidas International Marketing B.V.               │
    │      317968 │      317971 │  52.350000 │  52.500000 │ adidas International Marketing, B.V.              │ adidas International Marketing B.V.               │
    │           … │           … │          … │          … │ …                                                 │ …                                                 │
    └─────────────┴─────────────┴────────────┴────────────┴───────────────────────────────────────────────────┴───────────────────────────────────────────────────┘
    """  # noqa: E501
    j = join(left, right, condition, on_slow=on_slow, task=task, **kwargs)
    id_pairs = _distinct_record_ids(j)
    return _join_on_id_pairs(left, right, id_pairs)


def block_many(
    left: ir.Table,
    right: ir.Table,
    conditions: Iterable[_Condition],
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    labels: bool = False,
    **kwargs,
) -> ir.Table:
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
        See [check_join_algorithm()][mismo.block.check_join_algorithm]
        for more information.
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
    """
    conds = tuple(conditions)
    if not conds:
        raise ValueError("No conditions provided")

    def blk(rule):
        j = join(left, right, rule, on_slow=on_slow, task=task, **kwargs)
        ids = _distinct_record_ids(j)
        if labels:
            ids = ids.mutate(blocking_rule=_util.get_name(rule))
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
    left: ir.Table,
    right: ir.Table,
    condition: _Condition,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> ir.Table:
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
    j = _move_record_id_cols_first(j)
    return j


def _distinct_record_ids(t: ir.Table) -> ir.Table:
    return t["record_id_l", "record_id_r"].distinct()


def _join_on_id_pairs(left: ir.Table, right: ir.Table, id_pairs: ir.Table) -> ir.Table:
    left = left.rename("{name}_l")
    right = right.rename("{name}_r")
    j = id_pairs
    j = j.inner_join(right, "record_id_r")
    j = j.inner_join(left, "record_id_l")
    j = _move_record_id_cols_first(j)
    return j


def _move_record_id_cols_first(t: ir.Table) -> ir.Table:
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
    if callable(raw) and not isinstance(raw, Deferred):
        return _resolve_predicate(left, right, raw(left, right, **kwargs))
    keys_l = list(_util.bind(left, raw))
    keys_r = list(_util.bind(right, raw))
    left = left.mutate(keys_l)
    right = right.mutate(keys_r)
    keys_l = [left[val.get_name()] for val in keys_l]
    keys_r = [right[val.get_name()] for val in keys_r]
    cond = ibis.and_(*[lkey == rkey for lkey, rkey in zip(keys_l, keys_r)])
    return left, right, cond
