from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ibis.expr import types as ir

from mismo._util import optional_import

if TYPE_CHECKING:
    import numpy as np


def adjusted_mutual_info_score(
    labels_true: ir.Table,
    labels_pred: ir.Table,
    *,
    average_method: str = "arithmetic",
) -> float:
    """Adjusted Mutual Information between two clusterings.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.adjusted_mutual_info_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.adjusted_mutual_info_score(
        labels_true, labels_pred, average_method=average_method
    )


def adjusted_rand_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """Adjusted Rand Index between two clusterings.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.adjusted_rand_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.adjusted_rand_score(labels_true, labels_pred)


def fowlkes_mallows_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """Measure the similarity of two clusterings of a set of points.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.fowlkes_mallows_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.fowlkes_mallows_score(labels_true, labels_pred)


def completeness_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """Compute completeness metric of a cluster labeling given a ground truth.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.completeness_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.completeness_score(labels_true, labels_pred)


def homogeneity_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """Homogeneity metric of a cluster labeling given a ground truth.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.homogeneity_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.homogeneity_score(labels_true, labels_pred)


def v_measure_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """V-measure metric of a cluster labeling given a ground truth.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.v_measure_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.v_measure_score(labels_true, labels_pred)


def homogeneity_completeness_v_measure(
    labels_true: ir.Table, labels_pred: ir.Table, *, beta: float = 1.0
) -> tuple[float, float, float]:
    """Compute the homogeneity, completeness, and V-measure scores at once.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.homogeneity_completeness_v_measure][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.homogeneity_completeness_v_measure(
        labels_true, labels_pred, beta=beta
    )


def mutual_info_score(
    labels_true: ir.Table, labels_pred: ir.Table, *, contingency: Any = None
) -> float:
    """Compute the mutual information between two clusterings.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.mutual_info_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.mutual_info_score(labels_true, labels_pred, contingency=contingency)


def normalized_mutual_info_score(
    labels_true: ir.Table, labels_pred: ir.Table, *, average_method: str = "arithmetic"
) -> float:
    """Compute the normalized mutual information between two clusterings.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.normalized_mutual_info_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.normalized_mutual_info_score(
        labels_true, labels_pred, average_method=average_method
    )


def rand_score(labels_true: ir.Table, labels_pred: ir.Table) -> float:
    """Compute the Rand Index between two clusterings.

    The two input tables must have columns "record_id" and "label",
    a map from record ID to cluster label. They must have the same record IDs.

    See [sklearn.metrics.rand_score][] for more information.
    """
    with optional_import("scikit-learn"):
        from sklearn import metrics as _metrics
    labels_true, labels_pred = _to_numpy_labels(labels_true, labels_pred)
    return _metrics.rand_score(labels_true, labels_pred)


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
