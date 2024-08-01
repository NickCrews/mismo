""" A test module that verifies the string simiilarity functions return the same values as those in rapidfuzz"""

from rapidfuzz import fuzz
from mismo import text
from hypothesis import given, strategies as st


@given(x=st.text(), y=st.text())
def test_levenshtein_ratio(x,y):
    expected = fuzz.ratio(x,y)
    result = text.levenshtein_ratio(x,y).execute() * 100
    assert expected == result

@given(x=st.text(), y=st.text())
def test_token_set_ratio(x,y):
    expected = fuzz.token_set_ratio(x,y)
    result = text.token_set_ratio(x,y).execute()
    assert expected == result

@given(x=st.text(), y=st.text())
def test_token_sort_ratio(x,y):
    expected = fuzz.token_sort_ratio(x,y)
    result = text.token_sort_ratio(x,y).execute()
    assert expected == result

@given(x=st.text(), y=st.text())
def test_partial_token_sort_ratio(x,y):
    expected = fuzz.partial_token_sort_ratio(x,y)
    result = text.partial_token_sort_ratio(x,y).execute()
    assert expected == result