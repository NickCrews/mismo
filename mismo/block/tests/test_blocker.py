import pandas._testing as tm
import pytest
import vaex

from mismo.block import Equals, FingerprintBlocker


@pytest.fixture
def simple_df():
    records = [
        (0, "a", False),
        (1, "b", True),
        (2, "c", False),
        (3, "c", True),
    ]
    index, strs, bools = zip(*records)
    df = vaex.from_arrays(index=index, strings=strs, bools=bools)
    return df


def get_expected():
    columns = ["index_left", "index_right"]
    records = [
        (0, 0),
        (0, 2),
        (1, 1),
        (1, 3),
        (2, 0),
        (2, 2),
        (2, 3),
        (3, 1),
        (3, 2),
        (3, 3),
    ]
    serieses = zip(*records)
    arrs = dict(zip(columns, serieses))
    df = vaex.from_arrays(**arrs)
    return df


def test_basic_blocking(simple_df):
    predicates = [
        (Equals("bools"), Equals("bools")),
        (Equals("strings"), Equals("strings")),
    ]
    blocker = FingerprintBlocker(predicates)
    result = blocker.block(simple_df, simple_df)
    expected = get_expected()
    tm.assert_equal(result.to_pandas_df(), expected.to_pandas_df())
