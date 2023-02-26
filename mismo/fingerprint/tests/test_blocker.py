from __future__ import annotations

import ibis
from ibis.expr.types import Table
import pandas as pd
import pytest

from mismo._dataset import Dataset, DedupeDatasetPair
from mismo.fingerprint import Equals, FingerprintBlocker


@pytest.fixture
def simple_table() -> Table:
    records = [
        (0, "a", False),
        (1, "b", True),
        (2, "c", False),
        (3, "c", True),
    ]
    index, strs, bools = zip(*records)
    df = pd.DataFrame({"index": index, "strings": strs, "bools": bools})
    return ibis.memtable(df)


def test_basic_blocking(simple_table):
    predicates = [
        (Equals("bools"), Equals("bools")),
        (Equals("strings"), Equals("strings")),
    ]
    blocker = FingerprintBlocker(predicates)
    ds = Dataset(simple_table, "index")
    dsp = DedupeDatasetPair(ds)
    blocker.block(dsp)
    # TODO: actually test the result
