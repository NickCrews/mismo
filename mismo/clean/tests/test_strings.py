from __future__ import annotations

from ibis.expr import types as it
import pytest

from mismo.clean import _strings


@pytest.fixture
def string_column(column_factory) -> it.StringColumn:
    return column_factory(
        [
            "jane's   house",
            "Ross' house  ",
            "a",
            "",
            None,
            "bees\tall cook",
        ]
    )


def test_norm_whitespace(string_column):
    result = _strings.norm_whitespace(string_column)
    expected = ["jane's house", "Ross' house", "a", "", None, "bees all cook"]
    assert result.execute().tolist() == expected


@pytest.mark.parametrize(
    "inp,n,exp",
    [
        ("abc", 2, ["ab", "bc"]),
        ("abcd", 2, ["ab", "bc", "cd"]),
        ("abc", 3, ["abc"]),
        ("abcd", 3, ["abc", "bcd"]),
        (
            "abcdef",
            3,
            ["abc", "bcd", "cde", "def"],
        ),
        ("", 2, []),
        ("a", 2, []),
        (None, 4, None),
    ],
)
def test_ngrams(inp, n, exp):
    result = _strings.ngrams(inp, n).execute()
    if exp is None:
        assert result is None
    else:
        assert set(result) == set(exp)
