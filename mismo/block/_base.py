from __future__ import annotations

import abc
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from functools import cache
from textwrap import dedent
from typing import Any, Callable, Iterable, Literal, Union

import ibis
from ibis.expr.types import BooleanValue, Table

from mismo import _util
from mismo._util import format_table


class Blocking:
    _left: Table
    _right: Table
    _rules: Iterable[BlockingRule]

    def __init__(
        self,
        left: Table,
        right: Table,
        rules: BlockingRule | Iterable[BlockingRule],
        skip_rules: BlockingRule | Iterable[BlockingRule] = [],
    ) -> None:
        brs = [BlockingRule.make(r, left, right) for r in ibis.util.promote_list(rules)]
        if len(brs) == 0:
            raise ValueError("Blocking must have at least one rule")
        skips = [
            BlockingRule.make(r, left, right)
            for r in ibis.util.promote_list(skip_rules)
        ]
        if len(skips) > 0:
            skips_merged = or_(*skips)
            brs = [br - skips_merged for br in brs]

        self._left = left
        self._right = right
        self._rules = brs

    @property
    def left(self) -> Table:
        """The left Table"""
        return self._left

    @property
    def right(self) -> Table:
        """The right Table"""
        return self._right

    @property
    def rules(self) -> list[BlockingRule]:
        """The rules used to generate the blocking"""
        return list(self._rules)

    @property
    def ids(self) -> Table:
        """A table of (record_id_l, record_id_r) pairs"""
        return or_(*self._rules).ids

    @property
    def blocked(self) -> Table:
        """The left and right tables joined together

        A _l and _r suffix is added to each column"""
        b = or_(*self._rules).blocked
        return _order_blocked_data_columns(b)

    @cache
    def __repr__(self) -> str:
        template = dedent(
            f"""
            {self.__class__.__name__}(
                {{table}}
            )"""
        )
        return format_table(template, "table", self.blocked)

    def __len__(self) -> int:
        """The number of blocked pairs."""
        try:
            return self._len
        except AttributeError:
            self._len: int = self.ids.count().execute()
            return self._len

    def __hash__(self) -> int:
        return hash((self.left, self.right, self.ids))


_RuleIshAtomic = Union[BooleanValue, Literal[True], Table]
_RuleIsh = Union[
    _RuleIshAtomic,
    Callable[[Table, Table], _RuleIshAtomic],
]
RuleIsh = Union[_RuleIsh, list[_RuleIsh]]


class BlockingRule(abc.ABC):
    @classmethod
    def make(cls, rule: RuleIsh, left: Table, right: Table) -> BlockedRule:
        if isinstance(rule, BooleanValue) or rule is True:
            return ConditionRule(left, right, rule)
        elif isinstance(rule, Table):
            if set(rule.columns) == {"record_id_l", "record_id_r"}:
                return IdsRule(left, right, rule)
            else:
                return BlockedRule(left, right, rule)
        elif isinstance(rule, list):
            rules = [cls.make(r, left, right) for r in rule]
            if len(rules) == 0:
                raise ValueError("BlockingRule list must not be empty")
            return or_(*rules)
        else:
            result = rule(left, right)
            return cls.make(result, left, right)

    @property
    def ids(self) -> Table:
        """A table of (left_id, right_id) pairs"""
        raise NotImplementedError

    @property
    def blocked(self) -> Table:
        """The left and right Tables joined together on the blocked_ids"""
        raise NotImplementedError

    def __or__(self, other: Any) -> BlockingRule:
        raise NotImplementedError

    def __ror__(self, other: BlockingRule) -> IdsRule:
        return self.__or__(other)

    def __and__(self, other: BlockingRule) -> IdsRule:
        raise NotImplementedError

    def __rand__(self, other: BlockingRule) -> IdsRule:
        return self.__and__(other)

    def __sub__(self, other: BlockingRule) -> IdsRule:
        raise NotImplementedError

    def __rsub__(self, other: BlockingRule) -> IdsRule:
        return self.__sub__(other)


@dataclass(frozen=True)
class ConditionRule(BlockingRule):
    left: Table
    right: Table
    condition: _RuleIshAtomic = dataclass_field(repr=False)

    @property
    def ids(self) -> Table:
        return self.blocked["record_id_l", "record_id_r"]

    @property
    def blocked(self) -> Table:
        return _join_blocking(self.left, self.right, self.condition)

    def __or__(self, other: BlockingRule) -> BlockingRule:
        if isinstance(other, ConditionRule):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot OR two ConditionRules with different left/right tables"
                )
            return ConditionRule(
                self.left,
                self.right,
                self.condition | other.condition,
            )
        else:
            return NotImplemented

    def __and__(self, other: BlockingRule) -> IdsRule:
        if isinstance(other, ConditionRule):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot AND two ConditionRules with different left/right tables"
                )
            return ConditionRule(
                self.left,
                self.right,
                self.condition & other.condition,
            )
        else:
            return NotImplemented

    def __sub__(self, other: BlockingRule) -> IdsRule:
        if isinstance(other, ConditionRule):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot SUB two ConditionRules with different left/right tables"
                )
            return ConditionRule(
                self.left,
                self.right,
                self.condition & ~other.condition,
            )
        else:
            return NotImplemented


class _MaterializedRule(BlockingRule):
    def __init__(self, left: Table, right: Table) -> None:
        self.left = left
        self.right = right

    def __or__(self, other: BlockingRule) -> IdsRule:
        in_either = ibis.union(self.ids, other.ids, distinct=True)
        return IdsRule(self.left, self.right, in_either)

    def __and__(self, other: BlockingRule) -> IdsRule:
        in_both = ibis.intersect(self.ids, other.ids, distinct=True)
        return IdsRule(self.left, self.right, in_both)

    def __sub__(self, other: BlockingRule) -> IdsRule:
        in_self_not_other = ibis.difference(self.ids, other.ids, distinct=True)
        return IdsRule(self.left, self.right, in_self_not_other)

    def __rsub__(self, other: BlockingRule) -> IdsRule:
        in_other_not_self = ibis.difference(other.ids, self.ids, distinct=True)
        return IdsRule(self.left, self.right, in_other_not_self)


class IdsRule(_MaterializedRule):
    def __init__(self, left: Table, right: Table, ids: Table) -> None:
        super().__init__(left, right)
        self._ids = ids

    @property
    def ids(self) -> Table:
        return self._ids

    @property
    def blocked(self) -> Table:
        return _join_on_ids(self.left, self.right, self._ids)


class BlockedRule(_MaterializedRule):
    def __init__(self, left: Table, right: Table, blocked: Table) -> None:
        super().__init__(left, right)
        self._blocked = blocked

    @property
    def ids(self) -> Table:
        return self.blocked["record_id_l", "record_id_r"]

    @property
    def blocked(self) -> Table:
        return self._blocked


def or_(*args):
    if len(args) == 0:
        raise ValueError("Must provide at least one argument")
    result, *rest = args
    for r in rest:
        result = result | r
    return result


def _join_on_ids(left: Table, right: Table, id_pairs: Table) -> Table:
    """Join two tables based on a table of (record_id_l, record_id_r) pairs."""
    if set(id_pairs.columns) != {"record_id_l", "record_id_r"}:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
    left_cols = set(left.columns) - {"record_id"}
    right_cols = set(right.columns) - {"record_id"}
    lm = {c: c + "_l" for c in left_cols}
    rm = {c: c + "_r" for c in right_cols}

    result = id_pairs
    result = (
        result.inner_join(left, result.record_id_l == left.record_id)
        .relabel(lm)
        .drop("record_id")
    )
    result = (
        result.inner_join(right, result.record_id_r == right.record_id)
        .relabel(rm)
        .drop("record_id")
    )
    return result


def _join_blocking(left: Table, right: Table, predicates=tuple()) -> Table:
    """Join two tables, making every column end in _l or _r"""
    lc = set(left.columns)
    rc = set(right.columns)
    just_left = lc - rc
    just_right = rc - lc
    raw = _util.join(left, right, predicates, lname="{name}_l", rname="{name}_r")
    left_renaming = {c: c + "_l" for c in just_left}
    right_renaming = {c: c + "_r" for c in just_right}
    renaming = {**left_renaming, **right_renaming}
    return raw.relabel(renaming)


def _order_blocked_data_columns(t: Table) -> Table:
    cols = set(t.columns) - {"record_id_l", "record_id_r"}
    cols_in_order = ["record_id_l", "record_id_r", *sorted(cols)]
    return t[cols_in_order]
