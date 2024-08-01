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
        ("fuzzy was a bear", "fuzzy fuzzy was a bear", 100)
    ]
)
def test_token_set_ratio(string1, string2, expected):
    result = text.token_set_ratio(string1, string2).execute()
    assert expected == result

@pytest.mark.parametrize(
    "string1, string2, expected",
    [
        ("fuzzy wuzzy was a bear", "wuzzy fuzzy was a bear", 100),
        ("fuzzy was a bear", "fuzzy fuzzy was a bear", 84),

    ]
)
def test_token_sort_ratio(string1, string2, expected):
    result = text.token_sort_ratio(string1, string2).execute()
    assert expected == result


@pytest.mark.parametrize(
    "string1, string2, expected",
    [
        ("fuzzy wuzzy was a bear", "wuzzy fuzzy was a bear", 100),
        ("fuzzy was a bear", "fuzzy fuzzy was a bear", 100),
        ('great is scala', 'java is great', 81),
        ('C++ and Java', 'Java and Python', 64),
    ]
)
def test_partial_token_sort_ratio(string1, string2, expected):
    result = text.partial_token_sort_ratio(string1, string2).execute()
    assert expected == result