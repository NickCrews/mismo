from __future__ import annotations

from ibis.expr.types import Table
import pandas._testing as tm


def assert_tables_equal(left: Table, right: Table) -> None:
    assert left.schema() == right.schema()
    left_df = left.order_by(left.columns).to_pandas()
    right_df = right.order_by(left.columns).to_pandas()
    tm.assert_frame_equal(left_df, right_df)
