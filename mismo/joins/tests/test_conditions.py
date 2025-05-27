from __future__ import annotations

import ibis
from ibis.common.exceptions import IbisTypeError
import pytest

import mismo
from mismo import joins
from mismo.joins import _conditions


@pytest.mark.parametrize(
    "obj",
    [
        pytest.param("foo", id="str"),
        pytest.param(ibis._.foo, id="deferred"),
    ],
)
def test_key_join_condition(obj):
    condition = joins.join_condition(obj)
    assert isinstance(condition, _conditions.KeyJoinCondition)
    t1 = ibis.table({"foo": "int64"}, name="t1")
    t2 = ibis.table({"foo": "int64"}, name="t2")
    assert condition.__join_condition__(t1, t2).equals(t1.foo == t2.foo)

    tbad = ibis.table({"bar": "int64"}, name="tbad")
    with pytest.raises((IbisTypeError, AttributeError)):
        condition.__join_condition__(t1, tbad)


def test_deferred_left_right():
    condition = joins.join_condition(mismo.left.foo == mismo.right.bar)
    assert isinstance(condition, _conditions.LeftRightDeferredCondition)
    t1 = ibis.table({"foo": "int64"}, name="t1")
    t2 = ibis.table({"bar": "int64"}, name="t2")
    assert condition.__join_condition__(t1, t2).equals(t1.foo == t2.bar)

    tbad = ibis.table({"baz": "int64"}, name="tbad")
    with pytest.raises(AttributeError):
        condition.__join_condition__(t1, tbad)


def test_deferred_bogus():
    with pytest.raises(NotImplementedError):
        joins.join_condition(mismo.left.foo == ibis._.bar)
    with pytest.raises(NotImplementedError):
        joins.join_condition(mismo.left.foo == "hello")
