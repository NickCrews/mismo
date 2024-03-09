from __future__ import annotations

import ibis
import pytest

from mismo.vector import dot, norm


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
        pytest.param(
            [1],
            [4, 5],
            9999,
            id="array_length_mismatch",
            marks=pytest.mark.xfail(reason="fails at execution time"),
        ),
    ],
)
def test_dot(a, b, expected):
    result = dot(_to_vector(a), _to_vector(b))
    assert result.execute() == expected


@pytest.mark.parametrize(
    "a,metric,expected",
    [
        pytest.param(
            [1, 2], "l2", [0.4472135954999579, 0.8944271909999159], id="array_l2"
        ),
        pytest.param(
            [1, 2], "l1", [0.3333333333333333, 0.6666666666666666], id="array_l1"
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
    ],
)
def test_norm(a, metric, expected):
    result = norm(_to_vector(a), metric)
    assert result.execute() == expected
