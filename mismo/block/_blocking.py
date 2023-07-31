from __future__ import annotations

import collections
from functools import cache, cached_property
from typing import Any, Callable, Iterable, Literal, Protocol, Union, runtime_checkable

import ibis
from ibis.expr.types import BooleanValue, Table

from mismo.block._util import join, order_blocked_data_columns


class Blocking(Protocol):
    @property
    def left(self) -> Table:
        ...

    @property
    def right(self) -> Table:
        ...

    @property
    def ids(self) -> Table:
        """A table of (record_id_l, record_id_r) pairs"""
        ...

    @property
    def blocked(self) -> Table:
        """The left and right Tables blocked together with _l and _r suffixes"""
        ...

    @cache
    def __len__(self) -> int:
        """The number of blocked pairs."""
        return self.ids.count().execute()

    def __or__(self, other: Any) -> Blocking:
        return NotImplemented

    def __ror__(self, other: Blocking) -> IdsBlocking:
        return NotImplemented
        in_either = ibis.union(self.ids, other.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_either)

    def __and__(self, other: Blocking) -> Blocking:
        return NotImplemented

    def __rand__(self, other: Blocking) -> IdsBlocking:
        return NotImplemented
        in_both = ibis.intersect(self.ids, other.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_both)

    def __sub__(self, other: Blocking) -> Blocking:
        return NotImplemented

    def __rsub__(self, other: Blocking) -> IdsBlocking:
        return NotImplemented
        in_other_not_self = ibis.difference(other.ids, self.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_other_not_self)


__dispatches = []


def block(left: Table, right: Table, conditions) -> Blocking:
    for condition, factory in __dispatches:
        if condition(conditions):
            return factory(left, right, conditions)
    called = conditions(left, right)
    return block(left, right, called)


def register(condition: Callable[[Any], bool], factory: Callable[..., Blocking]):
    __dispatches.append((condition, factory))


_Condition = Union[BooleanValue, Literal[True]]
_RuleIshAtomic = Union[_Condition, Table]
_RuleIsh = Union[
    _RuleIshAtomic,
    Callable[[Table, Table], _RuleIshAtomic],
]
RuleIsh = Union[_RuleIsh, list[_RuleIsh]]


# class BaseBlocking(abc.ABC, Blocking):


@runtime_checkable
class PConditioned(Protocol):
    @property
    def condition(self) -> _Condition:
        ...


class _BaseBlocking(Blocking):
    def __init__(self, left: Table, right: Table) -> None:
        self._left = left
        self._right = right

    @property
    def left(self) -> Table:
        return self._left

    @property
    def right(self) -> Table:
        return self._right

    def __or__(self, other: Any) -> Blocking:
        return self.__ror__(other)

    def __ror__(self, other: Blocking) -> IdsBlocking:
        in_either = ibis.union(self.ids, other.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_either)

    def __and__(self, other: Blocking) -> Blocking:
        return self.__rand__(other)

    def __rand__(self, other: Blocking) -> IdsBlocking:
        in_both = ibis.intersect(self.ids, other.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_both)

    def __sub__(self, other: Blocking) -> Blocking:
        return self.__rsub__(other)

    def __rsub__(self, other: Blocking) -> IdsBlocking:
        in_other_not_self = ibis.difference(other.ids, self.ids, distinct=True)
        return IdsBlocking(self.left, self.right, in_other_not_self)


class ConditionBlocking(_BaseBlocking):
    def __init__(self, left: Table, right: Table, condition: _Condition) -> None:
        super().__init__(left, right)
        self.condition = condition

    @staticmethod
    def is_correct_type(arg: Any) -> bool:
        return isinstance(arg, BooleanValue) or arg is True

    @property
    def ids(self) -> Table:
        return self.blocked["record_id_l", "record_id_r"]

    @property
    def blocked(self) -> Table:
        return join(self.left, self.right, self.condition)

    def __or__(self, other: Blocking) -> Blocking:
        if isinstance(other, PConditioned):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot OR two ConditionRules with different left/right tables"
                )
            return ConditionBlocking(
                self.left,
                self.right,
                self.condition | other.condition,
            )
        else:
            return NotImplemented

    def __and__(self, other: Blocking) -> Blocking:
        if isinstance(other, PConditioned):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot AND two ConditionRules with different left/right tables"
                )
            return ConditionBlocking(
                self.left,
                self.right,
                self.condition & other.condition,
            )
        else:
            return NotImplemented

    def __not__(self) -> ConditionBlocking:
        return self.__class__(
            self.left,
            self.right,
            ~self.condition,
        )

    def __sub__(self, other: Blocking) -> IdsBlocking:
        if isinstance(other, PConditioned):
            if self.left is not other.left or self.right is not other.right:
                raise ValueError(
                    "Cannot SUB two ConditionRules with different left/right tables"
                )
            return ConditionBlocking(
                self.left,
                self.right,
                self.condition & ~other.condition,
            )
        else:
            return NotImplemented


register(ConditionBlocking.is_correct_type, ConditionBlocking)


class IdsBlocking(_BaseBlocking):
    @staticmethod
    def is_correct_type(arg: Any) -> bool:
        return isinstance(arg, Table) and set(arg.columns) == {
            "record_id_l",
            "record_id_r",
        }

    def __init__(self, left: Table, right: Table, ids: Table) -> None:
        super().__init__(left, right)
        self._ids = ids

    @property
    def ids(self) -> Table:
        return self._ids

    @property
    def blocked(self) -> Table:
        return _join_on_ids(self.left, self.right, self._ids)


register(IdsBlocking.is_correct_type, IdsBlocking)


class BlockedBlocking(_BaseBlocking):
    @staticmethod
    def is_correct_type(arg: Any) -> bool:
        return isinstance(arg, Table) and set(arg.columns) != {
            "record_id_l",
            "record_id_r",
        }

    def __init__(self, left: Table, right: Table, blocked: Table) -> None:
        super().__init__(left, right)
        self._blocked = blocked

    @property
    def ids(self) -> Table:
        return self.blocked["record_id_l", "record_id_r"]

    @property
    def blocked(self) -> Table:
        return self._blocked


register(BlockedBlocking.is_correct_type, BlockedBlocking)


class OrBlocking(_BaseBlocking):
    def __init__(self, left: Table, right: Table, rules: Iterable) -> None:
        super().__init__(left, right)
        self.inners = tuple(block(left, right, r) for r in rules)

    @cached_property
    def _inner(self) -> Blocking:
        return or_(*self.inners)

    @property
    def ids(self) -> Table:
        return self._inner.ids

    @property
    def blocked(self) -> Table:
        return self._inner.blocked


# From ibis.util
def is_iterable(o: Any) -> bool:
    """Return whether `o` is iterable and not a :class:`str` or :class:`bytes`.

    Parameters
    ----------
    o : object
        Any python object

    Returns
    -------
    bool

    Examples
    --------
    >>> is_iterable('1')
    False
    >>> is_iterable(b'1')
    False
    >>> is_iterable(iter('1'))
    True
    >>> is_iterable(i for i in range(1))
    True
    >>> is_iterable(1)
    False
    >>> is_iterable([])
    True
    """
    return not isinstance(o, (str, bytes)) and isinstance(o, collections.abc.Iterable)


register(is_iterable, OrBlocking)


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
    return order_blocked_data_columns(result)
