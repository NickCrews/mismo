import pyarrow as pa
import pytest
import vaex

from mismo.block import _strings


@pytest.fixture
def string_column(string_df):
    return string_df["strings"]


@pytest.fixture
def string_df():
    return vaex.from_arrays(
        strings=pa.array(
            ["jane's   house", "Ross' house  ", "a", "", None, "bees\tall cook"]
        )
    )


def test_norm_whitespace(string_column):
    result = _strings.norm_whitespace(string_column)
    expected = ["jane's house", "Ross' house", "a", "", None, "bees all cook"]
    assert result.tolist() == expected


def test_norm_possessives(string_column):
    result = _strings.norm_possessives(string_column)
    expected = ["janes   house", "Ross' house  ", "a", "", None, "bees\tall cook"]
    assert result.tolist() == expected


def test_TokenFingerprinter(string_df):
    fp = _strings.TokenFingerprinter(column="strings")
    result = fp.fingerprint(string_df)
    index, token = zip(
        *[
            (0, "janes"),
            (0, "house"),
            (1, "ross"),
            (1, "'"),
            (1, "house"),
            (2, "a"),
            (5, "bee"),  # We use the lemma
            (5, "all"),
            (5, "cook"),
        ]
    )
    expected = vaex.from_arrays(index=index, token=token)
    assert result.to_list() == expected.to_list()
