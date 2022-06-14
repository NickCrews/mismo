import pandas as pd
import pandas._testing as tm
import pytest

from mismo.block import _strings


@pytest.fixture
def string_series():
    return pd.Series(
        ["jane's   house", "Ross' house  ", "a", "", pd.NA, "bees\tall cook"],
        dtype="string[pyarrow]",
    )


@pytest.fixture
def string_df(string_series):
    return pd.DataFrame({"string": string_series})


def test_norm_whitespace(string_series):
    result = _strings.norm_whitespace(string_series)
    expected = pd.Series(
        ["jane's house", "Ross' house", "a", "", pd.NA, "bees all cook"],
        dtype="string[pyarrow]",
    )
    tm.assert_equal(result, expected)


def test_norm_possessives(string_series):
    result = _strings.norm_possessives(string_series)
    expected = pd.Series(
        ["janes   house", "Ross' house  ", "a", "", pd.NA, "bees\tall cook"],
        dtype="string[pyarrow]",
    )
    tm.assert_equal(result, expected)


def test_TokenFingerprinter(string_df):
    fp = _strings.TokenFingerprinter(column="string")
    result = fp.fingerprint(string_df)
    expected = pd.DataFrame(
        [
            (0, "janes"),
            (0, "house"),
            (1, "ross"),
            (1, "'"),
            (1, "house"),
            (2, "a"),
            (5, "bees"),
            (5, "all"),
            (5, "cook"),
        ],
        columns=["index", "token"],
    ).astype({"index": "int64", "token": "string[pyarrow]"})
    tm.assert_equal(result, expected)
