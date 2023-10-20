from __future__ import annotations

from typing import Callable, Iterator, Literal, Union

import ibis
from ibis import selectors as s
from ibis import _
from ibis.expr.types import BooleanValue, Table

from mismo.block import _util

_ConditionAtom = Union[BooleanValue, Literal[True]]
_Condition = Union[_ConditionAtom, Callable[[Table, Table], _ConditionAtom]]


class BlockingRule:
    """A rule for blocking two tables together."""

    def __init__(self, name: str, condition: _Condition) -> None:
        """Create a new blocking rule."""
        if not isinstance(name, str):
            raise TypeError(f"name must be a string, not {type(name)}")
        self._name = name
        self._condition = condition

    @property
    def name(self) -> str:
        """The name of the rule. Must be unique within a `BlockingRules`."""
        return self._name

    @property
    def condition(self) -> _Condition:
        """The condition that determines if two records should be blocked together."""
        return self._condition

    def block(self, left: Table, right: Table) -> Table:
        """Block two tables together."""
        # TODO: use EXPLAIN to warn if the condition causes a O(n^2) nested loop join
        # instead of the desired O(n) hash join
        return _util.join(left, right, self._condition)

    def __repr__(self) -> str:
        return f"BlockingRule({self.name})"


class BlockingRules:
    """An unordered, dict-like collection of `BlockingRule`s"""

    def __init__(self, *rules: BlockingRule) -> None:
        """Create a new collection of blocking rules.

        Each rule must have a unique name.
        """
        self._lookup: dict[str, BlockingRule] = {}
        for rule in rules:
            if rule.name in self._lookup:
                raise ValueError(f"Duplicate Rule name: {rule.name}")
            self._lookup[rule.name] = rule

    def block(
        self,
        left: Table,
        right: Table,
        *,
        labels: bool = False,
    ) -> Table:
        """Block two tables together using all the rules.

        Parameters
        ----------
        left
            The left table to block
        right
            The right table to block
        labels
            If False, the resulting table will only contain the columns of left and
            right. If True, a column of type `array<string>` will be added to the
            resulting table indicating which
            rules caused each record pair to be blocked.

            False is faster, because if a pair matches multiple rules we don't
            have to care about this. True is slower, because we need to test
            every rule, but this is useful for investigating the impact of each
            rule.

        Returns
        -------
        Table
            A table with all the columns of left (with a suffix of `_l`) and right
            (with a suffix of `_r`). Possibly with the labels column if `add_labels`
            is True.
        """

        def blk(rule: BlockingRule) -> Table:
            sub = rule.block(left, right)
            if labels:
                sub = sub.mutate(blocking_rule=rule.name)
            return sub

        sub_joined = [blk(rule) for rule in self]
        if labels:
            result = ibis.union(*sub_joined, distinct=False)
            result = result.group_by(~s.c("blocking_rule")).agg(
                blocking_rules=_.blocking_rule.collect()
            )
            result = result.relocate("blocking_rules", after="record_id_r")
        else:
            result = ibis.union(*sub_joined, distinct=True)
        return result

    def __getitem__(self, name: str) -> BlockingRule:
        """Get a rule by name."""
        return self._lookup[name]

    def __iter__(self) -> Iterator[BlockingRule]:
        """Iterate over the rules."""
        return iter(self._lookup.values())

    def __len__(self) -> int:
        """The number of rules."""
        return len(self._lookup)

    def __repr__(self) -> str:
        return f"BlockingRules({tuple(self)})"
