from __future__ import annotations

from ibis.expr import types as ir
import numpy as np
from sklearn import metrics as _metrics


def _wrap(metric):
    def wrapped(labels_true: ir.Table, labels_pred: ir.Table, **kwargs) -> float:
        labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
        return metric(labels_true, labels_pred, **kwargs)

    wrapped.__name__ = metric.__name__
    wrapped.__doc__ = metric.__doc__

    return wrapped


adjusted_mutual_info_score = _wrap(_metrics.adjusted_mutual_info_score)
adjusted_rand_score = _wrap(_metrics.adjusted_rand_score)
fowlkes_mallows_score = _wrap(_metrics.fowlkes_mallows_score)
completeness_score = _wrap(_metrics.completeness_score)
homogeneity_score = _wrap(_metrics.homogeneity_score)
v_measure_score = _wrap(_metrics.v_measure_score)
homogeneity_completeness_v_measure = _wrap(_metrics.homogeneity_completeness_v_measure)
mutual_info_score = _wrap(_metrics.mutual_info_score)
normalized_mutual_info_score = _wrap(_metrics.normalized_mutual_info_score)
rand_score = _wrap(_metrics.rand_score)


def _to_numpy_labels(
    labels_true: ir.Table, labels_pred: ir.Table
) -> tuple[np.ndarray, np.ndarray]:
    labels_true = labels_true["record_id", "label"]
    labels_pred = labels_pred["record_id", "label"]
    labels_true_df = labels_true.order_by("record_id").to_pandas()
    labels_pred_df = labels_pred.order_by("record_id").to_pandas()
    if not labels_true_df.record_id.equals(labels_pred_df.record_id):
        raise ValueError("labels_true and labels_pred must be aligned")
    return labels_true_df.label.values, labels_pred_df.label.values
