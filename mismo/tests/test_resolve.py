from __future__ import annotations

import ibis
import pytest

import mismo
from mismo import _resolve


@pytest.fixture
def left():
    return ibis.table(
        {
            "username": "string",
            "address": "string",
            "age": "int64",
            "is_true": "boolean",
        },
        name="left",
    )


@pytest.fixture
def right():
    return ibis.table(
        {
            "USERNAME": "string",
            "address": "string",
            "age": "int64",
        },
        name="right",
    )


@pytest.mark.parametrize(
    "condition",
    [
        pytest.param(
            ibis.and_(
                mismo.left.username == mismo.right.USERNAME,
                mismo.left.address == mismo.right.address.upper(),
                mismo.left.age == mismo.right.age + 1,
            ),
            id="flat",
        ),
        pytest.param(
            ibis.and_(
                mismo.left.username == mismo.right.USERNAME,
                ibis.and_(
                    mismo.left.address == mismo.right.address.upper(),
                    mismo.left.age == mismo.right.age + 1,
                ),
            ),
            id="nested",
        ),
    ],
)
def test_key_pair_resolvers_happy(condition, left, right):
    resolvers = _resolve.key_pair_resolvers(condition)
    assert len(resolvers) == 3
    (r1l, r1r), (r2l, r2r), (r3l, r3r) = resolvers
    # assert isinstance(r1, _resolve.IndividualDeferredResolver)
    assert r1l(left).equals(left.username)
    assert r1r(right).equals(right.USERNAME)
    # assert isinstance(r2, _resolve.IndividualDeferredResolver)
    assert r2l(left).equals(left.address)
    assert r2r(right).equals(right.address.upper())
    # assert isinstance(r3, _resolve.IndividualDeferredResolver)
    assert r3l(left).equals(left.age)
    assert r3r(right).equals(right.age + 1)


def test_key_pair_resolvers_single(left, right):
    condition = mismo.left.username == mismo.right.USERNAME
    resolvers = _resolve.key_pair_resolvers(condition)
    assert len(resolvers) == 1
    ((rl, rr),) = resolvers
    # assert isinstance(r1, _resolve.IndividualDeferredResolver)
    assert rl(left).equals(left.username)
    assert rr(right).equals(right.USERNAME)


def test_key_pair_resolvers_multiple_equals(left, right):
    condition = mismo.left.is_true == (mismo.right.USERNAME == "foo")
    resolvers = _resolve.key_pair_resolvers(condition)
    assert len(resolvers) == 1
    ((r1l, r1r),) = resolvers
    # assert isinstance(r1, _resolve.IndividualDeferredResolver)
    assert r1l(left).equals(left.is_true)
    assert r1r(right).equals(right.USERNAME == "foo")


def test_key_pair_resolvers_ors():
    condition = ibis.or_(
        mismo.left.username == mismo.right.USERNAME,
        mismo.left.address == mismo.right.address.upper(),
    )
    with pytest.raises(ValueError):
        _resolve.key_pair_resolvers(condition)


def test_key_pair_resolvers_nonequality():
    condition = ibis.and_(
        mismo.left.username == mismo.right.USERNAME,
        mismo.left.age > mismo.right.age + 1,
    )
    with pytest.raises(ValueError):
        _resolve.key_pair_resolvers(condition)
