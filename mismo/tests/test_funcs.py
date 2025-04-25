from __future__ import annotations

from mismo._funcs import is_binary, is_unary


def f1(x): ...


def f2(x, y=1): ...


def f3(x, /, y=1): ...


def f4(*args, y=1): ...


class MyCallable:
    def __call__(self): ...


class MyCallableWithParam:
    def __call__(self, x): ...


class MyCallableWithKwOnly:
    def __call__(self, *, x): ...


class MyCallableWithMissingDefaults:
    def __call__(self, x, y): ...


def test_is_unary():
    assert is_unary(1) is False
    assert is_unary(None) is False
    assert is_unary("string") is False

    assert is_unary(lambda: 1) is False
    assert is_unary(lambda *, x: x) is False
    assert is_unary(lambda x, y: x + y) is False
    assert is_unary(lambda x, y, z=2: x + y + z) is False
    assert is_unary(lambda x, *, y: x + y) is False

    assert is_unary(lambda x: x) is True
    assert is_unary(lambda x, y: x) is False
    assert is_unary(lambda x, /: x) is True
    assert is_unary(lambda x, /, y: x) is False
    assert is_unary(lambda *args: args) is True
    assert is_unary(lambda x, *args: args) is True
    assert is_unary(lambda x, y, *args: args) is False
    assert is_unary(lambda x, y=5, *args: args) is True
    assert is_unary(lambda x, y, *args, z: args) is False
    assert is_unary(lambda x, y, *args, z=5: args) is False
    assert is_unary(lambda x, y=1, z=2: x + y + z) is True
    assert is_unary(lambda x, *, y=1: x + y) is True
    assert is_unary(lambda *args, y=1: args) is True

    assert is_unary(f1) is True
    assert is_unary(f2) is True
    assert is_unary(f3) is True
    assert is_unary(f4) is True

    assert is_unary(len) is True
    assert is_unary(isinstance) is False

    assert is_unary(MyCallable()) is False
    assert is_unary(MyCallableWithKwOnly()) is False
    assert is_unary(MyCallableWithParam()) is True
    assert is_unary(MyCallableWithMissingDefaults()) is False


def test_is_binary():
    assert is_binary(1) is False
    assert is_binary(None) is False
    assert is_binary("string") is False

    assert is_binary(lambda: 1) is False
    assert is_binary(lambda *, x: x) is False
    assert is_binary(lambda x, y: x + y) is True
    assert is_binary(lambda x, y, z=2: x + y + z) is True
    assert is_binary(lambda x, *, y: x + y) is False

    assert is_binary(lambda x: x) is False
    assert is_binary(lambda x, y: x) is True
    assert is_binary(lambda x, /: x) is False
    assert is_binary(lambda x, /, y: x) is True
    assert is_binary(lambda *args: args) is True
    assert is_binary(lambda x, *args: args) is True
    assert is_binary(lambda x, y, *args: args) is True
    assert is_binary(lambda x, y=5, *args: args) is True
    assert is_binary(lambda x, y, *args, z: args) is False
    assert is_binary(lambda x, y, *args, z=5: args) is True
    assert is_binary(lambda x, y=1, z=2: x + y + z) is True
    assert is_binary(lambda x, *, y=1: x + y) is False
    assert is_binary(lambda *args, y=1: args) is True

    assert is_binary(f1) is False
    assert is_binary(f2) is True
    assert is_binary(f3) is True
    assert is_binary(f4) is True

    assert is_binary(len) is False
    assert is_binary(isinstance) is True

    assert is_binary(MyCallable()) is False
    assert is_binary(MyCallableWithKwOnly()) is False
    assert is_binary(MyCallableWithParam()) is False
    assert is_binary(MyCallableWithMissingDefaults()) is True
