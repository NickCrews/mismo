from __future__ import annotations

from ibis import _
import pytest

from mismo.block import KeyBlocker
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def inp(table_factory):
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
    return t


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("letter", id="str"),
        pytest.param(_.letter, id="deferred"),
    ],
)
def test_key_counts_letters(inp, table_factory, key):
    blocker = KeyBlocker(key)
    expected = table_factory(
        {
            "letter": ["c", "b", "a"],
            "expected_keys": [3, 3, 1],
            "expected_pairs_link": [9, 9, 1],
            "expected_pairs_dedupe": [3, 3, 0],
        }
    )
    result = blocker.key_counts(inp)
    assert_tables_equal(result, expected.select("letter", n=_.expected_keys))

    result2 = blocker.pair_counts(inp, inp)
    assert_tables_equal(result2, expected.select("letter", n=_.expected_pairs_dedupe))

    result2 = blocker.pair_counts(inp, inp, task="dedupe")
    assert_tables_equal(result2, expected.select("letter", n=_.expected_pairs_dedupe))

    result2 = blocker.pair_counts(inp, inp.view())
    assert_tables_equal(result2, expected.select("letter", n=_.expected_pairs_link))

    result2 = blocker.pair_counts(inp, inp, task="link")
    assert_tables_equal(result2, expected.select("letter", n=_.expected_pairs_link))


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(("letter", "num"), id="str"),
        pytest.param((_.letter, "num"), id="deferred"),
    ],
)
def test_key_counts_letters_num(inp, table_factory, keys):
    blocker = KeyBlocker(*keys)
    expected = table_factory(
        {
            "letter": ["c", "b", "b", "a"],
            "num": [3, 1, 2, 1],
            "expected_keys": [3, 2, 1, 1],
            "expected_pairs_link": [9, 4, 1, 1],
            "expected_pairs_dedupe": [3, 1, 0, 0],
        }
    )
    r = blocker.key_counts(inp)
    assert_tables_equal(r, expected.select("letter", "num", n=_.expected_keys))

    r = blocker.pair_counts(inp, inp)
    assert_tables_equal(r, expected.select("letter", "num", n=_.expected_pairs_dedupe))

    r = blocker.pair_counts(inp, inp, task="dedupe")
    assert_tables_equal(r, expected.select("letter", "num", n=_.expected_pairs_dedupe))

    r = blocker.pair_counts(inp, inp.view())
    assert_tables_equal(r, expected.select("letter", "num", n=_.expected_pairs_link))

    r = blocker.pair_counts(inp, inp, task="link")
    assert_tables_equal(r, expected.select("letter", "num", n=_.expected_pairs_link))
