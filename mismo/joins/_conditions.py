from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, Protocol, runtime_checkable

import ibis
from ibis import Deferred
from ibis.expr import types as ir

from mismo import _registry, _util


@runtime_checkable
class PJoinCondition(Protocol):
    """
    Has `__join_condition__(left: ibis.Table, right: ibis.Table)`, which returns something that `ibis.join()` understands.
    """  # noqa: E501

    def __join_condition__(self, left: ibis.Table, right: ibis.Table) -> Any:
        pass


join_condition = _registry.Registry[Callable[..., PJoinCondition], PJoinCondition]()


class BooleanJoinCondition:
    def __init__(self, boolean: bool | ir.BooleanValue):
        self.boolean = boolean

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> BooleanJoinCondition:
        if not isinstance(obj, (bool, ir.BooleanValue)):
            raise NotImplementedError
        return BooleanJoinCondition(obj)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue | bool:
        return self.boolean


class FuncJoinCondition:
    def __init__(self, func: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue]):
        self.func = func

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> FuncJoinCondition:
        if not callable(obj):
            raise NotImplementedError
        return FuncJoinCondition(obj)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self.func(left, right)


class AndJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        self.subconditions: tuple[PJoinCondition] = tuple(
            join_condition(c) for c in subconditions
        )

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> AndJoinCondition:
        tuple_subconditions = _try_iterable(obj)
        return AndJoinCondition(tuple_subconditions)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [c.__join_condition__(left, right) for c in self.subconditions]
        return ibis.and_(*conditions)


# KeyJoinCondition needs to be registered afterwards so that it has
# priority with 2-tuples
class MultiKeyJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        self.subconditions: tuple[KeyJoinCondition] = tuple(
            join_condition(c) for c in subconditions
        )

    @join_condition.register
    def _try(obj: Any) -> MultiKeyJoinCondition:
        subconditions = _try_iterable(obj)
        resolved = [join_condition(c) for c in subconditions]
        for sub in resolved:
            if not isinstance(sub, KeyJoinCondition):
                raise NotImplementedError
        return MultiKeyJoinCondition(resolved)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [c.__join_condition__(left, right) for c in self.subconditions]
        return ibis.and_(*conditions)


class BadKeyJoinCondition(ValueError):
    pass


class KeyJoinCondition:
    def __init__(self, spec: str | Deferred | tuple[str | Deferred, str | Deferred]):
        if isinstance(spec, (str, ibis.Deferred)):
            left_spec = right_spec = spec
        elif isinstance(spec, tuple):
            if len(spec) != 2:
                # This is eg ("name", "age", "address"), raise this so that
                # AndJoinCondition picks it up.
                raise BadKeyJoinCondition(spec)
            left_spec, right_spec = spec
        else:
            raise BadKeyJoinCondition(spec)

        if not isinstance(left_spec, (str, Deferred)):
            raise BadKeyJoinCondition(spec)
        if not isinstance(right_spec, (str, Deferred)):
            raise BadKeyJoinCondition(spec)
        self.left_spec = left_spec
        self.right_spec = right_spec

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> KeyJoinCondition:
        try:
            return KeyJoinCondition(obj)
        except BadKeyJoinCondition:
            raise NotImplementedError

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


def _try_iterable(obj: Any) -> tuple:
    try:
        return tuple(c for c in obj)
    except TypeError as e:
        if "is not iterable" in str(e):
            raise NotImplementedError
        raise
