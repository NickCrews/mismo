from __future__ import annotations

from typing import Callable, Literal, Protocol, runtime_checkable

from ibis.expr import types as ir

from mismo.block._core import join


@runtime_checkable
class PBlocker(Protocol):
    """A Callable that takes two tables and returns candidate pairs."""

    def __call__(self, left: ir.Table, right: ir.Table, **kwargs) -> ir.Table:
        """Return a table of candidate pairs.

        Implementers *must* expect to be called with two tables, `left` and `right`.
        Each one will have a column `record_id` that uniquely identifies each record.

        The returned table *must* follow the Mismo convention of blocked tables:
        - The left table *must* have columns suffixed with "_l".
        - The right table *must* have columns suffixed with "_r".
        - There *must* be no duplicate pairs.
        - There *can* be additional columns that could get used by later steps,
        such as a column of rule names used to generate the pair,
        or some kind of initial score.
        """


class CrossBlocker:
    """A Blocker that cross joins two tables, yielding all possible pairs."""

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ) -> ir.Table:
        return join(left, right, True, on_slow="ignore", task=task)


class EmptyBlocker:
    """A Blocker that yields no pairs."""

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ) -> ir.Table:
        return join(left, right, False, on_slow="ignore")


class ConditionBlocker:
    """Blocks based on a join predicate."""

    def __init__(
        self,
        condition: Callable[[ir.Table, ir.Table], ir.BooleanValue],
        *,
        name: str | None = None,
    ):
        """Create a Blocker that blocks based on a join condition.

        Parameters
        ----------
        condition
            A callable that takes two tables and returns a boolean condition.
        name
            The name of the rule, if any.
        """
        self.condition = condition
        self.name = name

    def __call__(self, left: ir.Table, right: ir.Table, **kwargs) -> ir.Table:
        return join(left, right, self.condition, **kwargs)
