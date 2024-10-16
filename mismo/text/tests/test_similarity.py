from __future__ import annotations

import numpy as np
import pytest

from mismo import sets, text


@pytest.mark.parametrize(
    "input, expected",
    [
        pytest.param("hello", ["HL", "HL"], id="hello"),
        pytest.param("world", ["ARLT", "FRLT"], id="world"),
        pytest.param("hello world", ["HLRLT", "HLRLT"], id="hello world"),
        pytest.param("catherine", ["K0RN", "KTRN"], id="catherine"),
        pytest.param("", ["", ""], id="empty"),
        pytest.param(None, None, id="empty"),
    ],
)
def test_double_metaphone(input, expected):
    result = text.double_metaphone(input).execute()
    assert expected == result


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("bare", "bone", 0.5),
        ("baz", "def", 0),
        ("", "", np.nan),
        (None, None, np.nan),
    ],
)
def test_levenshtein_ratio(string1, string2, expected):
    result = text.levenshtein_ratio(string1, string2).execute()
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert expected == result


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("foo bar", "foo", 0.3333),  # this is currently failing
        ("foo bar", "bar foo", 1),
    ],
)
def test_jaccard_string_similarity(string1, string2, expected):
    """Test that the string and set jaccard methods are equivalent."""
    result = text.jaccard(string1, string2).execute()
    tokens1 = text.tokenize(string1)
    tokens2 = text.tokenize(string2)
    set_result = sets.jaccard(tokens1, tokens2).execute()
    assert result == pytest.approx(set_result, 0.001)
    assert result == pytest.approx(expected, 0.001)
