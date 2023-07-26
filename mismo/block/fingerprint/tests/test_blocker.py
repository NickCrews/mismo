from __future__ import annotations

import ibis
from ibis.expr.types import Table
import pandas as pd
import pytest

from mismo.block.fingerprint import Equals, FingerprintBlocker


@pytest.fixture
def simple_table() -> Table:
    records = [
        (0, "a", False),
        (1, "b", True),
        (2, "c", False),
        (3, "c", True),
    ]
    record_ids, strs, bools = zip(*records)
    df = pd.DataFrame({"record_id": record_ids, "strings": strs, "bools": bools})
    return ibis.memtable(df)


def test_basic_blocking(simple_table):
    conditions = [
        (Equals("bools"), Equals("bools")),
        (Equals("strings"), Equals("strings")),
    ]
    blocker = FingerprintBlocker(conditions)
    blocker.block(simple_table, simple_table.view())
    # TODO: actually test the result
