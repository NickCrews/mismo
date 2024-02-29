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
    # need to sort after converting to pandas to avoid
    # https://github.com/ibis-project/ibis/issues/8442
    left_df = left.to_pandas().sort_values(order_by, ignore_index=True)
    right_df = right.to_pandas().sort_values(order_by, ignore_index=True)
    left_records = left_df.to_dict(orient="records")
    right_records = right_df.to_dict(orient="records")
    left_records = [make_record_approx(record) for record in left_records]
    right_records = [make_record_approx(record) for record in right_records]
    for i, (le, ri) in enumerate(zip(left_records, right_records)):
        assert le == ri, f"Row {i} does not match"


def make_record_approx(record: dict) -> dict:
    return {k: make_float_comparable(v) for k, v in record.items()}


def make_float_comparable(val):
    if pd.isna(val):
        return None
    if isinstance(val, float) and not pd.isna(val):
        return pytest.approx(val, rel=1e-4)
    return val
