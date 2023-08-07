from __future__ import annotations

from functools import update_wrapper

from ibis.expr.types import Table
import numpy as np
from sklearn import metrics as _metrics


def _wrap_sklearn_metric(metric):
    def wrapper(func):
        def wrapped(labels_true: Table, labels_pred: Table, **kwargs) -> float:
            labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
            return metric(labels_true, labels_pred, **kwargs)

        update_wrapper(wrapped, metric)

        return wrapped

    return wrapper


@_wrap_sklearn_metric(_metrics.adjusted_mutual_info_score)
def adjusted_mutual_info_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.adjusted_rand_score)
def adjusted_rand_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.calinski_harabasz_score)
def calinski_harabasz_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.davies_bouldin_score)
def davies_bouldin_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.completeness_score)
def completeness_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.cluster.contingency_matrix)
def contingency_matrix(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.cluster.pair_confusion_matrix)
def pair_confusion_matrix(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.fowlkes_mallows_score)
def fowlkes_mallows_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.homogeneity_completeness_v_measure)
def homogeneity_completeness_v_measure(
    labels_true: Table, labels_pred: Table, **kwargs
):
    pass


@_wrap_sklearn_metric(_metrics.homogeneity_score)
def homogeneity_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.mutual_info_score)
def mutual_info_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.normalized_mutual_info_score)
def normalized_mutual_info_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.rand_score)
def rand_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.silhouette_score)
def silhouette_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.silhouette_samples)
def silhouette_samples(labels_true: Table, labels_pred: Table, **kwargs):
    pass


@_wrap_sklearn_metric(_metrics.v_measure_score)
def v_measure_score(labels_true: Table, labels_pred: Table, **kwargs):
    pass


def _to_numpy_labels(
    labels_true: Table, labels_pred: Table
) -> tuple[np.ndarray, np.ndarray]:
    labels_true = labels_true["record_id", "label"]
    labels_pred = labels_pred["record_id", "label"]
    labels_true_df = labels_true.order_by("record_id").to_pandas()
    labels_pred_df = labels_pred.order_by("record_id").to_pandas()
    if not labels_true_df.record_id.equals(labels_pred_df.record_id):
        raise ValueError("labels_true and labels_pred must be aligned")
    return labels_true_df.label.values, labels_pred_df.label.values
