from __future__ import annotations

import ibis
import pytest

from mismo import clean


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
    result = clean.norm_whitespace(inp).execute()
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
    result = clean.ngrams(inp, n).execute()
    if exp is None:
        assert result is None
    else:
        assert set(result) == set(exp)
