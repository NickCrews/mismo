from __future__ import annotations

from ibis.expr.types import Table


def assert_tables_equal(left: Table, right: Table) -> None:
    assert left.schema() == right.schema()
    left_df = left.order_by(left.columns).to_pandas()
    right_df = right.order_by(left.columns).to_pandas()
    assert left_df.equals(right_df)
