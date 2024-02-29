from __future__ import annotations

from ibis.expr import types as it


def assert_tables_equal(
    left: it.Table,
    right: it.Table,
    *,
    order_by=None,
) -> None:
    assert dict(left.schema()) == dict(right.schema())
    # need to sort after converting to pandas to avoid
    # https://github.com/ibis-project/ibis/issues/8442
    if order_by is None:
        order_by = left.columns
    left_df = left.to_pandas().sort_values(order_by, ignore_index=True)
    right_df = right.to_pandas().sort_values(order_by, ignore_index=True)
    left_records = left_df.to_dict(orient="records")
    right_records = right_df.to_dict(orient="records")
    for i, (le, ri) in enumerate(zip(left_records, right_records)):
        assert le == ri, f"Row {i} does not match"
