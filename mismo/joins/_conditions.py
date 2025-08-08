from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, Protocol, TypeAlias, Union, runtime_checkable

import ibis
from ibis import Deferred
from ibis.common.deferred import var
from ibis.expr import types as ir

from mismo import _registry, _resolve, _util


@runtime_checkable
class HasJoinCondition(Protocol):
    """
    Has `__join_condition__(left: ibis.Table, right: ibis.Table)`, which returns something that `ibis.join()` understands.

    There are concrete implementations of this for various types of join conditions.
    For example, `BooleanJoinCondition` wraps a boolean
    or an `ibis.ir.BooleanValue` expression.
    """  # noqa: E501

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue | bool:
        """
        Given a left and right table, return something that `ibis.join()` understands.
        """
        pass


IntoHasJoinCondition: TypeAlias = Union[
    HasJoinCondition,
    bool,
    ibis.ir.BooleanValue,
    str,
    ibis.Deferred,
    Callable[[ibis.Table, ibis.Table], HasJoinCondition | bool | ibis.ir.BooleanValue],
    Iterable[
        HasJoinCondition,
        bool,
        ibis.ir.BooleanValue,
        str,
        ibis.Deferred,
        Callable[
            [ibis.Table, ibis.Table], HasJoinCondition | bool | ibis.ir.BooleanValue
        ],
    ],
]
"""
An object that can be converted into a HasJoinCondition with mismo.join_condition()
"""


class JoinConditionRegistry(
    _registry.Registry[Callable[..., HasJoinCondition], HasJoinCondition]
):
    """A registry for HasJoinCondition factory functions"""


_join_condition_registry = JoinConditionRegistry()


@_join_condition_registry.register
def _already_has_join_condition(obj: Any) -> HasJoinCondition:
    """If the object already has a `__join_condition__` method, return it as is."""
    if isinstance(obj, HasJoinCondition):
        return obj
    return NotImplemented


def join_condition(obj: IntoHasJoinCondition) -> HasJoinCondition:
    """
    Create a [HasJoinCondition][mismo.HasJoinCondition] from an object.

    Parameters
    ----------
    obj
        The object to create a join condition from.
        This can be anything that ibis understands as join condition,
        such as a boolean, an ibis.ir.BooleanValue expression, a `str`,
        an ibis.Deferred, etc.
        It also supports other types,
        such as `lambda left, right: <one of the above>`,

    Returns
    -------
        An object that follows the [HasJoinCondition][mismo.HasJoinCondition] protocol.
    """
    return _join_condition_registry(obj)


join_condition.register = _join_condition_registry.register


class BooleanJoinCondition:
    def __init__(self, boolean: bool | ir.BooleanValue):
        self.boolean = boolean

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> BooleanJoinCondition:
        if not isinstance(obj, (bool, ir.BooleanValue)):
            return NotImplemented
        return BooleanJoinCondition(obj)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue | bool:
        return self.boolean

    def __repr__(self):
        return f"{self.__class__.__name__}({self.boolean!r})"


class FuncJoinCondition:
    def __init__(self, func: Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue]):
        self.func = func

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> FuncJoinCondition:
        # Deffered's think they are callable, so guard against that.
        if isinstance(obj, ibis.Deferred):
            return NotImplemented
        if not callable(obj):
            return NotImplemented
        return FuncJoinCondition(obj)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self.func(left, right)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.func!r})"


class AndJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        self.subconditions: tuple[HasJoinCondition] = tuple(
            join_condition(c) for c in subconditions
        )

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> AndJoinCondition:
        tuple_subconditions = _try_iterable(obj)
        if tuple_subconditions is None:
            return NotImplemented
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
    """Join where all of the keys match."""

    def __init__(self, subconditions: Iterable[Any]):
        self.subconditions: tuple[KeyJoinCondition] = tuple(
            join_condition(c) for c in subconditions
        )

    @join_condition.register
    def _try(obj: Any) -> MultiKeyJoinCondition:
        subconditions = _try_iterable(obj)
        if subconditions is None:
            return NotImplemented
        resolved = [join_condition(c) for c in subconditions]
        for sub in resolved:
            if not isinstance(sub, KeyJoinCondition):
                return NotImplemented
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
        if isinstance(spec, str):
            left_spec = right_spec = spec
        elif isinstance(spec, Deferred):
            if _resolve.variables_names(spec) != {"_"}:
                raise BadKeyJoinCondition(
                    "Deferred join conditions must only contain `ibis._` variables."
                )
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
            return NotImplemented

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def bind_columns(
        self, left: ibis.Table, right: ibis.Table
    ) -> list[tuple[ibis.Column, ibis.Column]]:
        # print(self.left_spec, self.right_spec)
        l_cols = _util.bind(left, self.left_spec)
        r_cols = _util.bind(right, self.right_spec)
        # print(self.left_spec, self.right_spec)
        return list(zip(l_cols, r_cols, strict=True))

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanColumn:
        conditions = [coll == colr for coll, colr in self.bind_columns(left, right)]
        return ibis.and_(*conditions)


left = var("left")
"""A deferred placeholder for the left table in a join.

Examples
--------
>>> condition = mismo.left.last_name.upper() == mismo.right.family_name.upper()
>>> import ibis
>>> my_left_table = ibis.memtable([("johnson",), ("smith",)], columns=["last_name"])
>>> my_right_table = ibis.memtable([("JOHNSON",), ("JONES",)], columns=["family_name"])
>>> mismo.join(my_left_table, my_right_table, condition)
┏━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ last_name ┃ family_name ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ string    │ string      │
├───────────┼─────────────┤
│ johnson   │ JOHNSON     │
└───────────┴─────────────┘
"""
right = var("right")
"""A deferred placeholder for the right table in a join.

Examples
--------
>>> condition = mismo.left.last_name.upper() == mismo.right.family_name.upper()
>>> import ibis
>>> my_left_table = ibis.memtable([("johnson",), ("smith",)], columns=["last_name"])
>>> my_right_table = ibis.memtable([("JOHNSON",), ("JONES",)], columns=["family_name"])
>>> mismo.join(my_left_table, my_right_table, condition)
┏━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ last_name ┃ family_name ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ string    │ string      │
├───────────┼─────────────┤
│ johnson   │ JOHNSON     │
└───────────┴─────────────┘
"""


class LeftRightDeferredCondition:
    def __init__(self, condition: Deferred):
        if _resolve.variables_names(condition) != {"left", "right"}:
            raise ValueError(
                f"{self.__class__.__name__} must only contain 'left' and 'right'"
                "variables, eg `mismo.left.last_name == mismo.right.family_name`."
            )
        self.condition = condition

    @join_condition.register
    @staticmethod
    def _try(obj: Any) -> LeftRightDeferredCondition:
        if not isinstance(obj, Deferred):
            return NotImplemented
        try:
            return LeftRightDeferredCondition(obj)
        except ValueError:
            return NotImplemented

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self.condition.resolve(left=left, right=right)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.condition!r})"


def _try_iterable(obj: Any) -> tuple | None:
    if isinstance(obj, (str, bytes, bytearray, Deferred)):
        return None
    try:
        return tuple(c for c in obj)
    except TypeError as e:
        if "is not iterable" in str(e):
            return None
        raise
