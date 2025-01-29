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


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("müller", "muller"),
        ("Muñoz", "Munoz"),
        ("François", "Francois"),
        ("pérez", "perez"),
        ("à", "a"),
        ("á", "a"),
        ("â", "a"),
        ("ä", "a"),
        ("ã", "a"),
        ("å", "a"),
        ("Žižek", "Zizek"),
        ("Øslo", "Øslo"),  # unchanged
        ("østergaard", "østergaard"),  # unchanged
        ("æ", "æ"),  # unchanged
        ("ɑɽⱤoW", "ɑɽⱤoW"),  # unchanged
        ("aAaz;ZæÆ&", "aAaz;ZæÆ&"),  # unchanged
        ("ıI", "ıI"),  # unchanged
        ("", ""),
        (None, None),
    ],
)
def test_strip_accents(inp, expected):
    result = text.strip_accents(ibis.literal(inp, type="string")).execute()
    assert expected == result
