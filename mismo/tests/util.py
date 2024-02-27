from __future__ import annotations

from ibis.expr import types as it
import pandas._testing as tm


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
    try:
        tm.assert_frame_equal(left_df, right_df)
    except AssertionError:
        print(left_df)
        print(right_df)
        raise
