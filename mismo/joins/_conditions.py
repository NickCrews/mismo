from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, Protocol, runtime_checkable

import ibis
from ibis import Deferred
from ibis.expr import types as ir

from mismo import _registry, _util


@runtime_checkable
class PJoiner(Protocol):
    def __join__(
        self,
    ) -> Callable[[ibis.Table, ibis.Table], ibis.Table]:
        pass


@runtime_checkable
class PJoinCondition(Protocol):
    """
    Has `__join_condition__(left: ibis.Table, right: ibis.Table)`, which returns something that `ibis.join()` understands.
    """  # noqa: E501

    def __join_condition__(self, left: ibis.Table, right: ibis.Table) -> Any:
        pass


join_condition = _registry.Registry[Callable[..., PJoinCondition], PJoinCondition]()


@join_condition.register
class BooleanJoinCondition:
    def __init__(self, boolean: bool | ir.BooleanValue):
        if not isinstance(boolean, (bool, ir.BooleanValue)):
            raise NotImplementedError
        self.boolean = boolean

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue | bool:
        return self.boolean


@join_condition.register
class FuncJoinCondition:
    def __init__(self, func: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue]):
        if not callable(func):
            raise NotImplementedError
        self.func = func

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self.func(left, right)


@join_condition.register
class AndJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        try:
            self.subconditions: tuple[PJoinCondition] = tuple(
                join_condition(c) for c in subconditions
            )
        except TypeError as e:
            if "is not iterable" in str(e):
                raise NotImplementedError
            raise

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [c.__join_condition__(left, right) for c in self.subconditions]
        return ibis.and_(*conditions)


# KeyJoinCondition needs to be registered afterwards so that it has
# priority with 2-tuples
@join_condition.register
class MultiKeyJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        try:
            iterable_subconditions = tuple(c for c in subconditions)
        except TypeError as e:
            if "is not iterable" in str(e):
                raise NotImplementedError
            raise
        resolved = [join_condition(c) for c in iterable_subconditions]
        for sub in resolved:
            if not isinstance(sub, KeyJoinCondition):
                raise NotImplementedError
        self.subconditions: tuple[KeyJoinCondition] = resolved

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [c.__join_condition__(left, right) for c in self.subconditions]
        return ibis.and_(*conditions)


@join_condition.register
class KeyJoinCondition:
    def __init__(self, spec: str | Deferred | tuple[str | Deferred, str | Deferred]):
        if isinstance(spec, (str, ibis.Deferred)):
            left_spec = right_spec = spec
        elif isinstance(spec, tuple):
            if len(spec) != 2:
                # This is eg ("name", "age", "address"), raise this so that
                # AndJoinCondition picks it up.
                raise NotImplementedError
            left_spec, right_spec = spec
        else:
            raise NotImplementedError

        if not isinstance(left_spec, (str, Deferred)):
            raise NotImplementedError
        if not isinstance(right_spec, (str, Deferred)):
            raise NotImplementedError
        self.left_spec = left_spec
        self.right_spec = right_spec

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def bind_columns(
        self, left: ibis.Table, right: ibis.Table
    ) -> list[tuple[ibis.Column, ibis.Column]]:
        l_cols = _util.bind(left, self.left_spec)
        r_cols = _util.bind(right, self.right_spec)
        return list(zip(l_cols, r_cols))

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [coll == colr for coll, colr in self.bind_columns(left, right)]
        return ibis.and_(*conditions)
