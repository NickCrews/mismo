from __future__ import annotations

import ibis
from ibis.expr import datatypes as dt
import pandas as pd
import pytest

from mismo import arrays

mark_dtype = pytest.mark.parametrize("dtype", [dt.Int16(), dt.Float32()])


@mark_dtype
@pytest.mark.parametrize(
    "inp,exp",
    [
        pytest.param([1, 2, 3, None], 1, id="normal"),
        pytest.param([None, None], None, id="nulls"),
        pytest.param([], None, id="empty"),
        pytest.param(None, None, id="null"),
    ],
)
def test_array_min(backend, dtype, inp, exp):
    a = ibis.literal(inp).cast(dt.Array(value_type=dtype))
    result = arrays.array_min(a)
    assert result.type() == dtype
    r = result.execute()
    if pd.isna(r):
        r = None
    assert r == exp


@mark_dtype
@pytest.mark.parametrize(
    "inp,exp",
    [
        pytest.param([1, 2, 3, None], 3, id="normal"),
        pytest.param([None, None], None, id="nulls"),
        pytest.param([], None, id="empty"),
        pytest.param(None, None, id="null"),
    ],
)
def test_array_max(backend, dtype, inp, exp):
    a = ibis.literal(inp).cast(dt.Array(value_type=dtype))
    result = arrays.array_max(a)
    assert result.type() == dtype
    r = result.execute()
    if pd.isna(r):
        r = None
    assert r == exp


@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(arrays.array_median, id="median"),
        pytest.param(arrays.array_mean, id="mean"),
    ],
)
@pytest.mark.parametrize(
    "inp,type,exp",
    [
        pytest.param([0, 1, 2], "int", 1.0, id="happy"),
        pytest.param([0, 2], "int", 1.0, id="split"),
        pytest.param([0, 1], "int", 0.5, id="frac"),
        pytest.param([0.0, 1.0], "float", 0.5, id="frac_float"),
        pytest.param([0, 1, None, 2], "int", 1.0, id="with_null"),
        pytest.param([], "int", None, id="empty"),
        pytest.param(None, "int", None, id="null"),
        pytest.param([None, None], "int", None, id="all_null"),
    ],
)
def test_array_mean_median(backend, fn, inp, type, exp):
    a = ibis.literal(inp, type=f"array<{type}>")
    result = fn(a)
    assert result.type().is_floating()
    r = result.execute()
    if pd.isna(r):
        r = None
    assert r == exp


@pytest.mark.parametrize(
    "inp,exp",
    [
        pytest.param([True, False, True, None], False, id="mixed"),
        pytest.param([True, True], True, id="true"),
        pytest.param([True, True, None], True, id="true_null"),
        pytest.param([False, False], False, id="false"),
        pytest.param([False, False, None], False, id="false_null"),
        pytest.param([None, None], None, id="nulls"),
        pytest.param([], None, id="empty"),
        pytest.param(None, None, id="null"),
    ],
)
def test_array_all(backend, inp, exp):
    a = ibis.literal(inp, type="array<boolean>")
    result = arrays.array_all(a)
    assert result.type() == dt.Boolean()
    r = result.execute()
    assert r == exp


@pytest.mark.parametrize(
    "inp,exp",
    [
        pytest.param([True, False, True, None], True, id="mixed"),
        pytest.param([True, True], True, id="true"),
        pytest.param([True, True, None], True, id="true_null"),
        pytest.param([False, False], False, id="false"),
        pytest.param([False, False, None], False, id="false_null"),
        pytest.param([None, None], None, id="nulls"),
        pytest.param([], None, id="empty"),
        pytest.param(None, None, id="null"),
    ],
)
def test_array_any(backend, inp, exp):
    a = ibis.literal(inp, type="array<boolean>")
    result = arrays.array_any(a)
    assert result.type() == dt.Boolean()
    r = result.execute()
    assert r == exp


def test_array_sort():
    emails = ibis.array(
        [
            ibis.struct({"email": "b", "date": 1}),
            ibis.struct({"email": "c", "date": 3}),
            ibis.struct({"email": "a", "date": 2}),
        ]
    )
    result = arrays.array_sort(emails, key=lambda x: x.date)
    emails = result.map(lambda x: x.email)
    expected = ["b", "a", "c"]
    assert emails.execute() == expected
