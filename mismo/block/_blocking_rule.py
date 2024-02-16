from __future__ import annotations

from typing import Callable, Iterable, Literal, Union

from ibis.common.deferred import Deferred
from ibis.expr.types import BooleanValue, Column, Table

from mismo import _util
from mismo.block import _block

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
            - A callable that takes two tables and returns any of the above
        name
            The name of the rule. If not provided, a name will be generated based on
            the condition.
        """
        self._condition = condition
        self._name = name if name is not None else _util.get_name(condition)

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
        **kwargs,
    ) -> Table:
        return self.condition

    def block(self, left: Table, right: Table, **kwargs) -> Table:
        return _block.block(left, right, self.condition, **kwargs)

    def __repr__(self) -> str:
        return f"BlockingRule({self.get_name()})"
