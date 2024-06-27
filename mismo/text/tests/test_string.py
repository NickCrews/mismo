from __future__ import annotations

from mismo import text
from mismo.tests.util import assert_columns_equal


def test_levenshtein_ratio(table_factory, column_factory):
    string_1 = ["foo", "bar", "baz"]
    string_2 = [
        "foo",
        "baz",
        "def",
    ]
    t = table_factory({"string1": string_1, "string2": string_2})
    result = text.levenshtein_ratio(t.string1, t.string2).execute()
    expected_ratios = [1, 2 / 3, 0]
    expected = column_factory(expected_ratios).execute()
    assert_columns_equal(result, expected, tol=1e-6)
