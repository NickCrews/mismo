from __future__ import annotations

import ibis
import pandas as pd
import pytest

from mismo import vector


def _to_vector(x):
    if isinstance(x, dict):
        return ibis.literal(x, type="map<string, int64>")
    if isinstance(x, list):
        return ibis.literal(x, type="array<int64>")
    return x


@pytest.mark.parametrize(
    "a,b,expected",
    [
        pytest.param([1, 2], [4, 5], 14.0, id="array_int"),
        pytest.param([1.0, 2.0], [4, 5], 14.0, id="array_float"),
        pytest.param({"a": 1, "b": 2}, {"b": 3, "c": 4}, 6.0, id="map"),
        pytest.param([], [], 0, id="empty_arrays"),
        pytest.param({}, {}, 0, id="empty_maps"),
        pytest.param({}, {"a": 5}, 0, id="one_empty_map"),
        pytest.param(
            [1],
            [4, 5],
            9999,
            id="array_length_mismatch",
            marks=pytest.mark.xfail(reason="fails at execution time"),
        ),
        pytest.param(
            ibis.literal(None, "map<string, int64>"),
            {"a": 5},
            None,
            id="null_nonull_map",
        ),
        pytest.param(
            {"a": 5},
            ibis.literal(None, "map<string, int64>"),
            None,
            id="nonnull_null_map",
        ),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            [5, 3],
            None,
            id="one_null_array",
        ),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            ibis.literal(None, "array<int64>"),
            None,
            id="both_null_array",
        ),
        pytest.param(
            ibis.literal(None, "map<string, int64>"),
            ibis.literal(None, "map<string, int64>"),
            None,
            id="both_null_map",
        ),
    ],
)
def test_dot(a, b, expected):
    result = vector.dot(_to_vector(a), _to_vector(b)).execute()
    if pd.isna(result):
        result = None
    assert result == expected


@pytest.mark.parametrize(
    "a,b,expected",
    [
        pytest.param([1, 1], [-2, -2], -1.0, id="opposite_directions"),
        pytest.param([1, 2], [2, 4], 1.0, id="same_directions"),
        pytest.param([1], [-99], -1.0, id="1D_arrays"),
        pytest.param(
            [],
            [],
            0.0,
            id="empty_arrays",
            marks=pytest.mark.xfail(
                reason="https://github.com/duckdb/duckdb/issues/12960"
            ),
        ),
        pytest.param(
            {},
            {},
            0.0,
            id="empty_maps",
            marks=pytest.mark.xfail(
                reason="https://github.com/duckdb/duckdb/issues/12960"
            ),
        ),
        pytest.param(
            {},
            {"a": 5},
            0.0,
            id="one_empty_map",
            marks=pytest.mark.xfail(
                reason="https://github.com/duckdb/duckdb/issues/12960"
            ),
        ),
        pytest.param({"a": 1, "b": 1}, {"a": -2, "b": -2}, -1.0, id="opposite_maps"),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            [5, 3],
            None,
            id="one_null_array",
        ),
        pytest.param(
            [5, 3],
            ibis.literal(None, "array<int64>"),
            None,
            id="nonnull_null_array",
        ),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            ibis.literal(None, "array<int64>"),
            None,
            id="both_null_array",
        ),
    ],
)
def test_cosine_similarity(a, b, expected):
    def to_ibis(x):
        if isinstance(x, dict):
            return ibis.literal(x, type="map<string, int64>")
        if isinstance(x, list):
            return ibis.literal(x, type="array<int64>")
        return x

    a = to_ibis(a)
    b = to_ibis(b)
    e = vector.cosine_similarity(a, b)
    result = e.execute()
    if pd.isna(result):
        result = None
    assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "a,metric,expected",
    [
        pytest.param([-3, 4], "l2", 5.0, id="array_l2"),
        pytest.param([3, 4], "l1", 7, id="array_l1"),
        pytest.param({"a": -3, "b": 4}, "l2", 5.0, id="map_l2"),
        pytest.param({"a": -3, "b": 4}, "l1", 7, id="map_l1"),
        pytest.param([], "l2", None, id="array_empty"),
        pytest.param({}, "l2", None, id="map_empty"),
        pytest.param(ibis.literal(None, "array<int64>"), "l2", None, id="null_array"),
        pytest.param(
            ibis.literal(None, "map<string, int64>"), "l1", None, id="null_map_l1"
        ),
        pytest.param(
            ibis.literal(None, "map<string, int64>"), "l2", None, id="null_map_l2"
        ),
    ],
)
def test_norm(a, metric, expected):
    e = vector.norm(_to_vector(a), metric=metric)
    result = e.execute()
    if pd.isna(result):
        result = None
    assert result == expected


@pytest.mark.parametrize(
    "a,metric,expected",
    [
        pytest.param(
            [-1, 2], "l2", [-0.4472135954999579, 0.8944271909999159], id="array_l2"
        ),
        pytest.param(
            [-1, 2], "l1", [-0.3333333333333333, 0.6666666666666666], id="array_l1"
        ),
        pytest.param(
            {"a": 1, "b": 2},
            "l2",
            {"a": 0.4472135954999579, "b": 0.8944271909999159},
            id="map_l2",
        ),
        pytest.param(
            {"a": 1, "b": 2},
            "l1",
            {"a": 0.3333333333333333, "b": 0.6666666666666666},
            id="map_l1",
        ),
        pytest.param([], "l2", [], id="array_empty"),
        pytest.param({}, "l2", {}, id="map_empty"),
        pytest.param(ibis.literal(None, "array<int64>"), "l2", None, id="null_array"),
        pytest.param(
            ibis.literal(None, "map<string, int64>"), "l1", None, id="null_map_l1"
        ),
        pytest.param(
            ibis.literal(None, "map<string, int64>"), "l2", None, id="null_map_l2"
        ),
    ],
)
def test_normalize(a, metric, expected):
    result = vector.normalize(_to_vector(a), metric=metric)
    assert result.execute() == expected


@pytest.mark.parametrize(
    "a,b,expected",
    [
        pytest.param([1, 2], [4, 5], [4, 10], id="array_int"),
        pytest.param([1.0, 2.0], [4, 5], [4.0, 10.0], id="array_float"),
        pytest.param([], [], [], id="array_empty"),
        pytest.param(
            [1],
            [4, 5],
            9999,
            id="array_length_mismatch",
            marks=pytest.mark.xfail(reason="fails at execution time"),
        ),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            [5, 3],
            None,
            id="array_nonnull_null",
        ),
        pytest.param(
            [5, 3],
            ibis.literal(None, "array<int64>"),
            None,
            id="array_null_nonnull",
        ),
        pytest.param(
            ibis.literal(None, "array<int64>"),
            ibis.literal(None, "array<int64>"),
            None,
            id="array_both_null",
        ),
        pytest.param({"a": 1, "b": 2}, {"b": 3, "c": 4}, {"b": 6}, id="map_int"),
        pytest.param(
            {"a": 1.0, "b": 2.0}, {"b": 3.0, "c": 4.0}, {"b": 6.0}, id="map_float"
        ),
        pytest.param({}, {}, {}, id="map_empty"),
        pytest.param({}, {"a": 5}, {}, id="map_empty_nonempty"),
        pytest.param(
            ibis.literal(None, "map<string, int64>"),
            {"a": 5},
            None,
            id="map_null_nonull",
        ),
        pytest.param(
            {"a": 5},
            ibis.literal(None, "map<string, int64>"),
            None,
            id="map_nonnull_null",
        ),
        pytest.param(
            ibis.literal(None, "map<string, int64>"),
            ibis.literal(None, "map<string, int64>"),
            None,
            id="map_both_null",
        ),
    ],
)
def test_mul(a, b, expected):
    result = vector.mul(_to_vector(a), _to_vector(b)).execute()
    if expected is None:
        assert pd.isna(result)
    else:
        assert result == expected
