from __future__ import annotations

import ibis
import numpy as np
import pytest

from mismo import text


@pytest.mark.parametrize(
    "inp,exp",
    [
        ("jane's   house", "jane's house"),
        ("Ross' house  ", "Ross' house"),
        ("a", "a"),
        ("", ""),
        (" ", ""),
        (None, None),
        ("bees\tall cook", "bees all cook"),
    ],
)
def test_norm_whitespace(inp, exp):
    inp = ibis.literal(inp, type="string")
    result = text.norm_whitespace(inp).execute()
    assert result == exp


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
    result = text.ngrams(inp, n).execute()
    if exp is None:
        assert result is None
    else:
        assert set(result) == set(exp)


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("bar", "baz", 2 / 3),
        ("baz", "def", 0),
        ("", "", np.nan),
        (None, None, np.nan),
    ],
)
def test_levenshtein_ratio(string1, string2, expected):
    result = text.levenshtein_ratio(string1, string2).execute()
    if not np.isnan(expected):
        assert abs(result - expected) <= 1e-6
    else:
        assert np.isnan(result)
