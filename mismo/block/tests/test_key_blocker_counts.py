from __future__ import annotations

import ibis
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
def test_counts_letters(inp, table_factory, key):
    blocker = KeyBlocker(key)
    expected = table_factory(
        {
            "letter": ["c", "b", "a"],
            "expected_keys": [3, 3, 1],
            "expected_pairs_link": [9, 9, 1],
            "expected_pairs_dedupe": [3, 3, 0],
        }
    )
    c = blocker.key_counts(inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_keys))
    assert c.n_total == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)

    c = blocker.pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_dedupe))
    assert c.n_total == 6

    c = blocker.pair_counts(inp, inp, task="dedupe")
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_dedupe))

    c = blocker.pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_link))

    c = blocker.pair_counts(inp, inp, task="link")
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_link))


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(("letter", "num"), id="str"),
        pytest.param((_.letter, "num"), id="deferred"),
    ],
)
def test_counts_letters_num(inp, table_factory, keys):
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
    c = blocker.key_counts(inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_keys))
    assert c.n_total == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)

    c = blocker.pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_dedupe))
    assert c.n_total == 4

    c = blocker.pair_counts(inp, inp, task="dedupe")
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_dedupe))

    c = blocker.pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_link))

    c = blocker.pair_counts(inp, inp, task="link")
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_link))


def test_counts_empty(inp, table_factory):
    # null keys will never get blocked with any others
    blocker = KeyBlocker(ibis.null(int))
    c = blocker.pair_counts(inp, inp)
    assert c.count().execute() == 0
    assert c.n_total == 0
