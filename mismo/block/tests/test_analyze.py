from __future__ import annotations

from ibis import _
import pytest

from mismo.block import estimate_n_pairs, key_counts
from mismo.tests.util import assert_tables_equal


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("letter", id="str"),
        pytest.param(_.letter, id="deferred"),
    ],
)
def test_key_counts_letters(table_factory, key):
    t = table_factory(
        {
            "letter": ["a", "b", "c", "b", "c", None, "c"],
        }
    )
    expected = table_factory(
        {
            "letter": ["c", "b", "a"],
            "n": [3, 2, 1],
        }
    )
    result = key_counts(t, key)
    assert_tables_equal(result, expected)


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(("letter", "num"), id="str"),
        pytest.param((_.letter, "num"), id="deferred"),
    ],
)
def test_key_counts_letters_num(table_factory, key):
    records = [
        ("a", 1),
        ("b", 1),
        ("b", 1),
        ("c", 3),
        ("b", 2),
        ("c", 3),
        (None, 4),
        ("c", 3),
    ]
    letters, nums = zip(*records)
    t = table_factory({"letter": letters, "num": nums})
    expected = table_factory(
        {
            "letter": ["c", "b", "b", "a"],
            "num": [3, 1, 2, 1],
            "n": [3, 2, 1, 1],
        }
    )
    result = key_counts(t, key)
    assert_tables_equal(result, expected)


def test_estimate_n_pairs(table_factory):
    records = [
        ("a", 1),
        ("b", 2),
        ("c", 3),
        ("b", 1),
        ("c", 3),
        (None, 4),
        ("c", 3),
    ]
    letters, nums = zip(*records)
    t = table_factory({"letter": letters, "num": nums})
    expected = 12  # 1 + 1 + 1 + 9
    result = estimate_n_pairs(t, t, ("letter", "num")).execute()
    assert result == expected
