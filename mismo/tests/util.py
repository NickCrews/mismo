from __future__ import annotations

from typing import Literal

from ibis.expr import types as it
import pandas as pd
import pytest


def assert_tables_equal(
    left: it.Table,
    right: it.Table,
    *,
    column_order: Literal["ignore", "exact"] = "exact",
    order_by=None,
) -> None:
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
    for i, (le, ri) in enumerate(zip(left_records, right_records)):
        assert le == ri, f"Row {i} does not match"


def make_record_approx(record: dict) -> dict:
    return {k: make_float_comparable(v) for k, v in record.items()}


def make_float_comparable(val):
    if isinstance(val, dict):
        return {k: make_float_comparable(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [make_float_comparable(v) for v in val]
    if pd.isna(val):
        return None
    if isinstance(val, float) and not pd.isna(val):
        return pytest.approx(val, rel=1e-4)
    return val
