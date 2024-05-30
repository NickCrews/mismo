from __future__ import annotations

from ibis.expr import types as ir
import pytest

from mismo import cluster


@pytest.fixture
def labels_true(table_factory) -> ir.Table:
    return table_factory(
        {
            "record_id": [0, 1, 2, 3],
            "label": [0, 0, 0, 1],
        }
    )


@pytest.fixture
def labels_pred(table_factory) -> ir.Table:
    return table_factory(
        {
            "record_id": [0, 1, 2, 3],
            "label": [0, 0, 1, 1],
        }
    )


@pytest.mark.parametrize(
    "func",
    [
        cluster.rand_score,
        cluster.adjusted_rand_score,
        cluster.fowlkes_mallows_score,
        cluster.homogeneity_score,
        cluster.completeness_score,
        cluster.v_measure_score,
        cluster.mutual_info_score,
        cluster.normalized_mutual_info_score,
    ],
)
def test_scalar_metrics(func, labels_true: ir.Table, labels_pred: ir.Table):
    # We assume sklearns implementation is correct
    result = func(labels_true, labels_pred)
    assert result >= 0.0
    assert result <= 1.0
    assert func.__doc__ is not None


def test_homogeneity_completeness_v_measure(
    labels_true: ir.Table, labels_pred: ir.Table
):
    # We assume sklearns implementation is correct
    homogeneity, completeness, v_measure = cluster.homogeneity_completeness_v_measure(
        labels_true, labels_pred
    )
    assert homogeneity >= 0.0
    assert homogeneity <= 1.0
    assert completeness >= 0.0
    assert completeness <= 1.0
    assert v_measure >= 0.0
    assert v_measure <= 1.0
