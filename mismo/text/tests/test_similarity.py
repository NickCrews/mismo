from __future__ import annotations

import numpy as np
import pytest

from mismo import text


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
        ("foo bar", "foo", 0.3333),
        ("foo bar", "bar foo", 1),
        ("foo foo bar", "foo baz", 0.7143),
        ("foo", "", 0),
        (None, "foo", np.nan),
    ],
)
def test_jaccard_string_similarity_character(string1, string2, expected):
    result = text.jaccard(string1, string2, tokenize="by_character").execute()
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert result == pytest.approx(expected, 0.001)


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("foo bar", "foo", 0.5),
        ("foo bar", "bar foo", 1),
        ("foo foo bar", "foo baz", 0.333),
        ("foo", "", 0),
        (None, "foo", np.nan),
    ],
)
def test_jaccard_string_similarity_word(string1, string2, expected):
    result = text.jaccard(string1, string2, tokenize="on_whitespace").execute()
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert result == pytest.approx(expected, 0.001)


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("foo", "food", 0.942),
        ("bar", "bim", 0.5555),
        ("a", "", 0),
        ("", "", 0),
        ("foo", None, np.nan),
        (None, None, np.nan),
    ],
)
def test_jaro_winkler_similarity(string1, string2, expected):
    result = text.jaro_winkler_similarity(string1, string2).execute()
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert result == pytest.approx(expected, 0.001)


@pytest.mark.parametrize(
    "string1,string2,expected",
    [
        ("foo", "foo", 1),
        ("foo", "food", 0.916),
        ("bar", "bim", 0.5555),
        ("a", "", 0),
        ("", "", 0),
        ("foo", None, np.nan),
        (None, None, np.nan),
    ],
)
def test_jaro_similarity(string1, string2, expected):
    result = text.jaro_similarity(string1, string2).execute()
    if np.isnan(expected):
        assert np.isnan(result)
    else:
        assert result == pytest.approx(expected, 0.001)
