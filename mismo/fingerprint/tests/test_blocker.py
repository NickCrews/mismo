import ibis
import pandas as pd
import pytest
from ibis.expr.types import Table

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
    blocker.block(simple_table)
    # TODO: actually test the result
