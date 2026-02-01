from __future__ import annotations

import ibis
from ibis.common.exceptions import IbisTypeError
import pytest

import mismo
from mismo import joins
from mismo.joins import _conditions

t1 = ibis.table({"foo": "int64", "bar": "string", "baz": "int64"}, name="t1")
t2 = ibis.table({"foo": "int64", "bar": "string", "qux": "int64"}, name="t2")


@pytest.mark.parametrize(
    "obj",
    [
        pytest.param(True, id="True"),
        pytest.param(False, id="False"),
        pytest.param(ibis.literal(True), id="expr_True"),
        pytest.param(ibis.literal(False), id="expr_False"),
    ],
)
def test_boolean_condition(obj):
    condition = joins.join_condition(obj)
    assert isinstance(condition, _conditions.BooleanJoinCondition)
    assert condition.__join_condition__(t1, t2) is obj


@pytest.mark.parametrize(
    "obj",
    [
        pytest.param("foo", id="str"),
        pytest.param(ibis._.foo, id="deferred"),
        pytest.param(lambda t: t.foo, id="unary"),
        pytest.param((ibis._.foo, lambda t: t.foo), id="tuple2"),
    ],
)
def test_key_join_condition(obj):
    condition = joins.join_condition(obj)
    assert isinstance(condition, _conditions.KeyJoinCondition)
    assert condition.__join_condition__(t1, t2).equals(t1.foo == t2.foo)

    tbad = ibis.table({"bar": "int64"}, name="tbad")
    with pytest.raises((IbisTypeError, AttributeError)):
        condition.__join_condition__(t1, tbad)

    assert condition.left_resolver(t1).equals(t1.foo)
    assert condition.right_resolver(t2).equals(t2.foo)


@pytest.mark.parametrize(
    ("obj", "expected"),
    [
        pytest.param([], ibis.literal(True), id="empty list"),
        pytest.param(["foo"], t1.foo == t2.foo, id="list1"),
        pytest.param(
            ["foo", "bar"],
            ibis.and_(t1.foo == t2.foo, t1.bar == t2.bar),
            id="list2",
        ),
        pytest.param(
            [(lambda t: t.foo, "foo"), "bar"],
            ibis.and_(t1.foo == t2.foo, t1.bar == t2.bar),
            id="tuple_in_list",
        ),
    ],
)
def test_and_join_condition(obj, expected):
    condition = joins.join_condition(obj)
    assert isinstance(condition, _conditions.AndJoinCondition)
    actual = condition.__join_condition__(t1, t2)
    assert actual.equals(expected)


def test_function_binary():
    condition = joins.join_condition(lambda left, right: left.foo == right.foo)
    assert condition.__join_condition__(t1, t2).equals(t1.foo == t2.foo)


def test_left_right_deferred():
    condition = joins.join_condition(mismo.left.foo == mismo.right.qux)
    assert isinstance(condition, _conditions.LeftRightDeferredCondition)
    assert condition.__join_condition__(t1, t2).equals(t1.foo == t2.qux)

    tbad = ibis.table({"baz": "int64"}, name="tbad")
    with pytest.raises(AttributeError):
        condition.__join_condition__(t1, tbad)


def test_deferred_bogus():
    with pytest.raises(_conditions.BadKeyJoinCondition):
        joins.join_condition(mismo.left.foo == ibis._.bar)
    with pytest.raises(_conditions.BadKeyJoinCondition):
        joins.join_condition(mismo.left.foo == "hello")
