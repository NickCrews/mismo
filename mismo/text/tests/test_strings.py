from __future__ import annotations

import ibis
import pytest

from mismo import text
from mismo.tests.util import assert_columns_equal


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


def test_levenshtein_ratio(table_factory):
    string_1 = ["foo", "bar", "baz", "", None]
    string_2 = [
        "foo",
        "baz",
        "def",
        "",
        None
    ]
    t = table_factory(
        {"string1": string_1, "string2": string_2, "expected": [1, 2 / 3, 0, 1, None]}
    )
    t = t.mutate(result=text.levenshtein_ratio(t.string1, t.string2))
    assert_columns_equal(t.result, t.expected, tol=1e-6)
