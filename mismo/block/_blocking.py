from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

import ibis
from ibis import _
from ibis import selectors as s
from ibis.common.deferred import Deferred
from ibis.expr.types import BooleanValue, Column, Table

from mismo import _join, _util
from mismo.block._util import join as _block_join

# Something that can be used to reference a column in a table
_ColumnReferenceLike = Union[
    str,
    Deferred,
    Callable[[Table], Column],
]
# Something that can be used as a condition in a join between two tables
_ConditionAtom = Union[
    BooleanValue,
    Literal[True],
    tuple[_ColumnReferenceLike, _ColumnReferenceLike],
]
_ConditionOrConditions = Union[
    _ConditionAtom,
    Iterable[_ConditionAtom],
]
_Condition = Union[
    _ConditionOrConditions,
    Callable[[Table, Table], _ConditionOrConditions],
]


def block(
    left: Table,
    right: Table,
    conditions,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    labels: bool = False,
    **kwargs,
) -> Table:
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

        - Anything that ibis accepts as a join predicate
        - A Table, which is assumed to be the result of a join, and will be used as-is
        - A callable that takes two tables and returns one of the above

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
        See [check_join_type()][mismo.block.check_join_type] for more information.
    labels
        If False, the resulting table will only contain the columns of left and
        right. If True, a column of type `array<string>` will be added to the
        resulting table indicating which
        rules caused each record pair to be blocked.

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

    def blk(rule: _Condition) -> Table:
        resolved_predicates = _join.resolve_predicates(left, right, rule)
        if len(resolved_predicates) == 1 and isinstance(resolved_predicates[0], Table):
            sub = resolved_predicates[0]
        else:
            sub = _block_join(
                left, right, resolved_predicates, on_slow=on_slow, **kwargs
            )
        if labels:
            sub = sub.mutate(blocking_rule=_get_name(rule))
        return sub

    sub_joined = [blk(rule) for rule in conds]
    if labels:
        result = ibis.union(*sub_joined, distinct=False)
        result = result.group_by(~s.c("blocking_rule")).agg(
            blocking_rules=_.blocking_rule.collect()
        )
        result = result.relocate("blocking_rules", after="record_id_r")
    else:
        result = ibis.union(*sub_joined, distinct=True)
    return result


def _to_conditions(x) -> list[_Condition]:
    if isinstance(x, tuple) and len(x) == 2:
        return [x]
    return _util.promote_list(x)


def _get_name(x: _Condition) -> str:
    try:
        return x.get_name()
    except AttributeError:
        return str(x)


class BlockingRule:
    """A rule for blocking two tables together."""

    def __init__(self, condition: _Condition, *, name: str | None = None) -> None:
        """Create a new blocking rule.

        Parameters
        ----------
        condition
            The condition that determines if two records should be blocked together.
            This can be any of the following:

            - anything that ibis accepts as a join predicate
            - tuple[str, str]
            - tuple[Column, Column]
            - tuple[Deferred, Deferred]
            - A callable that takes two tables and returns any of the above
        name
            The name of the rule.
        """
        self._condition = condition
        if isinstance(name, str):
            self._name = name
        else:
            self._name = _get_name(condition)

    def get_name(self) -> str:
        """The name of the rule."""
        return self._name

    @property
    def condition(self) -> _Condition:
        """The condition that determines if two records should be blocked together."""
        return self._condition

    def __call__(
        self,
        left: Table,
        right: Table,
        *,
        on_slow: Literal["error", "warn", "ignore"] = "error",
        **kwargs,
    ) -> Table:
        # return _util.join(left, right, self.condition, on_slow=on_slow)
        return self.condition

    def block(self, left: Table, right: Table, **kwargs) -> Table:
        return block(left, right, self, **kwargs)

    def __repr__(self) -> str:
        return f"BlockingRule({self.get_name()})"


# class BlockingRules:
#     """An unordered, dict-like collection of `BlockingRule`s"""

#     def __init__(self, *rules: BlockingRule) -> None:
#         """Create a new collection of blocking rules.

#         Each rule must have a unique name.
#         """
#         self._lookup: dict[str, BlockingRule] = {}
#         for rule in rules:
#             if rule.name in self._lookup:
#                 raise ValueError(f"Duplicate Rule name: {rule.name}")
#             self._lookup[rule.name] = rule

#     def __call__(
#         self,
#         left: Table,
#         right: Table,
#         *,
#         on_slow: Literal["error", "warn", "ignore"] = "error",
#         labels: bool = False,
#         **kwargs,
#     ) -> Table:
#         """Block two tables together using all the rules.

#         Parameters
#         ----------
#         left
#             The left table to block
#         right
#             The right table to block
#         labels
#             If False, the resulting table will only contain the columns of left and
#             right. If True, a column of type `array<string>` will be added to the
#             resulting table indicating which
#             rules caused each record pair to be blocked.

#             False is faster, because if a pair matches multiple rules we don't
#             have to care about this. True is slower, because we need to test
#             every rule, but this is useful for investigating the impact of each
#             rule.

#         Returns
#         -------
#         Table
#             A table with all the columns of left (with a suffix of `_l`) and right
#             (with a suffix of `_r`). Possibly with the labels column if `add_labels`
#             is True.
#         """

#         def blk(rule: BlockingRule) -> Table:
#             sub = rule(left, right, on_slow=on_slow, **kwargs)
#             if labels:
#                 sub = sub.mutate(blocking_rule=rule.name)
#             return sub

#         sub_joined = [blk(rule) for rule in self]
#         if labels:
#             result = ibis.union(*sub_joined, distinct=False)
#             result = result.group_by(~s.c("blocking_rule")).agg(
#                 blocking_rules=_.blocking_rule.collect()
#             )
#             result = result.relocate("blocking_rules", after="record_id_r")
#         else:
#             result = ibis.union(*sub_joined, distinct=True)
#         return result

#     def __getitem__(self, name: str) -> BlockingRule:
#         """Get a rule by name."""
#         return self._lookup[name]

#     def __iter__(self) -> Iterator[BlockingRule]:
#         """Iterate over the rules."""
#         return iter(self._lookup.values())

#     def __len__(self) -> int:
#         """The number of rules."""
#         return len(self._lookup)

#     def __repr__(self) -> str:
#         return f"BlockingRules({tuple(self)})"
