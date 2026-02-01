from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable, Protocol, TypeAlias, Union, runtime_checkable

import ibis
from ibis import Deferred
from ibis.common.deferred import var
from ibis.expr import types as ir

from mismo import _funcs, _resolve
from mismo._resolve import IntoValueResolver, value_resolver


@runtime_checkable
class HasJoinCondition(Protocol):
    """
    Has `__join_condition__(left: ibis.Table, right: ibis.Table) -> bool | ibis.ir.BooleanValue`.

    There are concrete implementations of this for various types of join conditions.
    For example, `BooleanJoinCondition` wraps a boolean
    or an `ibis.ir.BooleanValue` expression.
    """  # noqa: E501

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> bool | ibis.ir.BooleanValue:
        """
        Given a left and right table, return something that `ibis.join()` understands.
        """
        pass


_IntoJoinConditionAtom: TypeAlias = Union[
    HasJoinCondition,
    bool,
    ibis.ir.BooleanValue,
    IntoValueResolver,
    Callable[[ibis.Table, ibis.Table], HasJoinCondition],
]
IntoJoinCondition: TypeAlias = Union[
    _IntoJoinConditionAtom,
    Iterable[_IntoJoinConditionAtom],
]
"""
An object that `mismo.join_condition()` can convert into a `HasJoinCondition`.
"""


def join_condition(obj: IntoJoinCondition) -> HasJoinCondition:
    """
    Create a [HasJoinCondition][mismo.HasJoinCondition] from an object.

    The HasJoinCondition protocol defines a single method,
    `__join_condition__(left: ibis.Table, right: ibis.Table) -> bool | ibis.ir.BooleanValue`,
    which can be used to resolve a join condition given two tables.

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

    Examples
    --------
    >>> import mismo
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> t1 = ibis.memtable({"name": ["alice", "alice"], "age": [30, 40]})
    >>> t2 = ibis.memtable({"name": ["alice", "bob"], "age": [30, 50]})
    >>> conditioner = mismo.join_condition("name")
    >>> t1.join(t2, conditioner.__join_condition__(t1, t2))
    ┏━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
    ┃ name   ┃ age   ┃ age_right ┃
    ┡━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
    │ string │ int64 │ int64     │
    ├────────┼───────┼───────────┤
    │ alice  │    30 │        30 │
    │ alice  │    40 │        30 │
    └────────┴───────┴───────────┘

    Note that for this example, you could have used the `mismo.join()` function,
    which uses this under the hood:
    >>> mismo.join(t1, t2, "name")
    ┏━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
    ┃ name   ┃ age   ┃ age_right ┃
    ┡━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
    │ string │ int64 │ int64     │
    ├────────┼───────┼───────────┤
    │ alice  │    30 │        30 │
    │ alice  │    40 │        30 │
    └────────┴───────┴───────────┘
    """
    if isinstance(obj, HasJoinCondition):
        return obj
    if isinstance(obj, (bool, ir.BooleanValue)):
        return BooleanJoinCondition(obj)
    if isinstance(obj, ibis.Deferred):
        if _resolve.variables_names(obj) == {"left", "right"}:
            return LeftRightDeferredCondition(obj)
        return KeyJoinCondition(obj)
    if isinstance(obj, str):
        return KeyJoinCondition(obj)
    if _funcs.is_unary(obj):
        return KeyJoinCondition(obj)
    if _funcs.is_binary(obj):
        return BinaryFuncJoinCondition(obj)
    tuple_subconditions = _try_iterable(obj)
    if tuple_subconditions is not None:
        if len(tuple_subconditions) == 2 and isinstance(obj, tuple):
            return KeyJoinCondition(obj)
        return AndJoinCondition(tuple_subconditions)
    raise TypeError(f"Can't convert object of type {type(obj)} to a HasJoinCondition")


class BooleanJoinCondition:
    def __init__(self, boolean: bool | ir.BooleanValue, /):
        self.boolean = boolean

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue | bool:
        return self.boolean

    def __repr__(self):
        return f"{self.__class__.__name__}({self.boolean!r})"


class BinaryFuncJoinCondition:
    def __init__(self, func: Callable[[ibis.Table, ibis.Table], IntoJoinCondition], /):
        self.func = func

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> bool | ibis.ir.BooleanValue:
        raw = self.func(left, right)
        return join_condition(raw).__join_condition__(left, right)

    def __repr__(self):
        return f"{self.__class__.__name__}({self.func!r})"


class AndJoinCondition:
    def __init__(self, subconditions: Iterable[Any]):
        self.subconditions: tuple[HasJoinCondition] = tuple(
            join_condition(c) for c in subconditions
        )

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> bool | ibis.ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(
        self, left: ibis.Table, right: ibis.Table
    ) -> bool | ibis.ir.BooleanValue:
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
                raise BadKeyJoinCondition(spec)
            left_spec, right_spec = spec
        elif _funcs.is_unary(spec):
            left_spec = right_spec = spec
        else:
            raise BadKeyJoinCondition(spec)

        try:
            self.left_resolver = value_resolver(left_spec)
        except ValueError as e:
            raise BadKeyJoinCondition(spec) from e
        try:
            self.right_resolver = value_resolver(right_spec)
        except ValueError as e:
            raise BadKeyJoinCondition(spec) from e

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        return self.join_condition(left, right)

    def join_condition(self, left: ibis.Table, right: ibis.Table) -> ir.BooleanValue:
        return self.left_resolver(left) == self.right_resolver(right)


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
