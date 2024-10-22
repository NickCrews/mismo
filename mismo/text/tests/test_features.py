from __future__ import annotations

import ibis
import pytest

from mismo import text


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
    "inp,exp",
    [
        ("abc", ["abc"]),
        ("abc def", ["abc", "def"]),
        ("abc  def", ["abc", "def"]),
        ("  abc  def", ["abc", "def"]),
        (" ", []),
        ("", []),
        (None, None),
    ],
)
def test_tokenize(inp, exp):
    result = text.tokenize(ibis.literal(inp, type=str)).execute()
    if exp is None:
        assert result is None
    else:
        assert result == exp
