from __future__ import annotations

from typing import Literal

import ibis
from ibis.expr import types as ir
import pandas as pd
import pytest


def assert_tables_equal(
    left: ir.Table,
    right: ir.Table,
    *,
    column_order: Literal["ignore", "exact"] = "exact",
    on_schema_mismatch: Literal["error", "cast_to_left", "cast_to_right"] = "error",
    order_by=None,
) -> None:
    if dict(left.schema()) != dict(right.schema()):
        if on_schema_mismatch == "error":
            assert dict(left.schema()) == dict(right.schema())
        if on_schema_mismatch == "cast_to_left":
            right = right.cast(left.schema())
        elif on_schema_mismatch == "cast_to_right":
            left = left.cast(right.schema())
        else:
            raise ValueError(f"Invalid on_schema_mismatch: {on_schema_mismatch}")
    assert dict(left.schema()) == dict(right.schema())
    if column_order == "exact":
        assert left.columns == right.columns
    elif column_order == "ignore":
        left = left[left.columns]
        right = right[left.columns]
    else:
        raise ValueError(f"Invalid column_order: {column_order}")

    if order_by is None:
        order_by = left.columns
    # need to cache before ordering to avoid
    # https://github.com/ibis-project/ibis/issues/8442
    left = left.cache().order_by(order_by)
    right = right.cache().order_by(order_by)
    left_records = left.to_pandas().to_dict(orient="records")
    right_records = right.to_pandas().to_dict(orient="records")
    left_records = [make_record_approx(record) for record in left_records]
    right_records = [make_record_approx(record) for record in right_records]
    assert left_records == right_records


def make_record_approx(record: dict) -> dict:
    return {k: make_float_comparable(v) for k, v in record.items()}


def make_float_comparable(val):
    if isinstance(val, dict):
        return {k: make_float_comparable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [make_float_comparable(v) for v in val]
    if pd.isna(val):
        return None
    if isinstance(ibis.literal(val), ir.FloatingValue) and not pd.isna(val):
        return pytest.approx(val, rel=1e-3)
    return val


def get_clusters(
    cluster_id: ibis.Column, *, label: ibis.Column | None = None
) -> set[frozenset]:
    """Convert a label column into a set of clusters.

    Say you have a table of records, and one of the columns
    is an ID that groups records together.
    This function will return a set of frozensets, where each
    frozenset is a cluster of record IDs.

    You can either provide a label column to act as the record IDs,
    or if not given, it will use `ibis.row_number()`.
    """
    if label is None:
        t = cluster_id.name("cluster_id").as_table()
        t = t.mutate(label=ibis.row_number())
        clusters = (
            t.group_by("cluster_id").aggregate(clusters=t.label.collect()).clusters
        )
    else:
        clusters = label.collect().over(group_by=cluster_id)

    def make_hashable(cluster):
        for record_id in cluster:
            if isinstance(record_id, dict):
                yield tuple(record_id.values())
            else:
                yield record_id

    return {frozenset(make_hashable(c)) for c in clusters.execute()}
