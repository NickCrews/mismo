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
        # duckdb maybe is inconsistent with how it takes the median of
        # even-length arrays. Skip testing that for now.
        # SELECT MEDIAN(x) from (SELECT unnest([0.0, 2.0]) as x); -> 0.0
        # SELECT MEDIAN(x) from (SELECT unnest([0, 2]) as x); -> 1.0
        # This sounds to me like the exact inverse of the docs,
        # which say that ordinal values get floored and quantitative values get meaned.
        # https://discord.com/channels/909674491309850675/921073327009853451/1278856039164411916
        pytest.param([0, 1, 2], "int", 1.0, id="happy"),
        pytest.param([0, 2], "int", 1.0, id="split"),
        pytest.param([0, 1], "int", 0.5, id="frac"),
        pytest.param([0.0, 1.0, 2.0], "float", 1.0, id="frac_float"),
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


@pytest.mark.parametrize(
    "key,exp",
    [
        pytest.param(None, ["a", "a", "b", "c"], id="none"),
        pytest.param((lambda x: x.date), ["b", "a", "c", "a"], id="lambda"),
        pytest.param(
            (lambda x: [x.date, x.email]), ["b", "a", "c", "a"], id="lambda_multiple"
        ),
        pytest.param(ibis._.date, ["b", "a", "c", "a"], id="deferred"),
        pytest.param(
            [ibis._.date, ibis._.email], ["b", "a", "c", "a"], id="date_then_email"
        ),
        pytest.param(
            [ibis._.email, ibis._.date], ["a", "a", "b", "c"], id="email_then_date"
        ),
    ],
)
def test_array_sort(key, exp):
    emails = ibis.array(
        [
            ibis.struct({"email": "b", "date": 1}),
            ibis.struct({"email": "c", "date": 3}),
            ibis.struct({"email": "a", "date": 2}),
            ibis.struct({"email": "a", "date": 4}),
        ]
    )
    result = arrays.array_sort(emails, key=key)
    emails = result.map(lambda x: x.email)
    assert emails.execute() == exp


@pytest.mark.parametrize(
    "other",
    [
        pytest.param([2], id="normal"),
        pytest.param([1, 2, 3], id="remove_nothing"),
        pytest.param([], id="remove_all"),
    ],
)
@pytest.mark.parametrize(
    "inp",
    [
        pytest.param([1, 2, 3], id="normal"),
        pytest.param([1, 2, 2], id="duplicates"),
        pytest.param([2, None, 2], id="duplicates_null"),
        pytest.param([1], id="single"),
        pytest.param([1, None, 2], id="mixed"),
        pytest.param([None, 1], id="start_null"),
        pytest.param([1, None], id="end_null"),
        pytest.param([], id="empty"),
        pytest.param(None, id="null"),
    ],
)
def test_array_filter_isin_other(table_factory, column_factory, inp, other):
    # We make 5 rows (admittedly, all with the same data)
    # because the implementation does some unnesting logic
    # so we want to test that the output rows line up with the input rows
    pairs = [{"inp": inp} for _ in range(5)]
    t = table_factory(pairs, schema={"inp": "array<int32>"})
    other_col = column_factory(other, type="int32")
    filtered = arrays.array_filter_isin_other(
        t, "inp", other_col, result_format="filtered"
    )
    df = filtered.execute()

    def baseline(inp):
        if inp is None:
            return None
        return [x for x in inp if x in other or pd.isna(x)]

    assert len(df) == len(pairs)
    assert df.filtered.tolist() == [baseline(x) for x in df.inp.tolist()]
