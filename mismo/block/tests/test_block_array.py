from __future__ import annotations

from ibis import _
from ibis.expr.types import Table
import pandas as pd
import pytest

from mismo.block import ArrayBlocker, block
from mismo.tests.util import assert_tables_equal


@pytest.mark.parametrize(
    "left,right",
    [
        ("arrays", _.arrays),
        ("arrays", lambda t: t.arrays),
    ],
)
def test_array_blocker(table_factory, t1: Table, t2: Table, left, right):
    blocked = block(t1, t2, ArrayBlocker(left, right))
    blocked_ids = blocked[["record_id_l", "record_id_r"]]
    expected = table_factory(
        pd.DataFrame(
            [(0, 90), (1, 90)],
            columns=["record_id_l", "record_id_r"],
        )
    )
    assert_tables_equal(expected, blocked_ids)
