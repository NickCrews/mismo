from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo.linker import KeyLinker
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def inp(table_factory):
    records = [
        (1, "a", 1),
        (2, "b", 1),
        (3, "b", 1),
        (4, "c", 3),
        (5, "b", 2),
        (6, "c", 3),
        (7, None, 4),
        (8, "c", 3),
    ]
    # letters, nums = zip(*records)
    # t = table_factory({"letter": letters, "num": nums})
    t = table_factory(
        records, schema={"record_id": "int64", "letter": "string", "num": "int64"}
    )
    return t


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("letter", id="str"),
        pytest.param(_.letter, id="deferred"),
    ],
)
def test_counts_letters(inp, table_factory, key):
    linker = KeyLinker(key)
    expected = table_factory(
        {
            "record_id": [0, 1, 2],
            "letter": ["c", "b", "a"],
            "expected_keys": [3, 3, 1],
            "expected_pairs_link": [9, 9, 1],
            "expected_pairs_dedupe": [3, 3, 0],
        }
    )
    c = linker.key_counts_left(inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_keys))
    assert c.n_total() == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)
    c = linker.key_counts_right(inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_keys))
    assert c.n_total() == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)

    c = linker.pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_dedupe))
    assert c.n_total() == 6

    linker = KeyLinker(key, task="dedupe")
    c = linker.pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_dedupe))

    linker = KeyLinker(key, task="link")
    c = linker.pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_link))

    linker = KeyLinker(key, task="link")
    c = linker.pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", n=_.expected_pairs_link))


@pytest.mark.parametrize(
    "keys",
    [
        pytest.param(("letter", "num"), id="str"),
        pytest.param((_.letter, "num"), id="deferred"),
    ],
)
def test_counts_letters_num(inp, table_factory, keys):
    linker = KeyLinker(keys)
    expected = table_factory(
        {
            "record_id": [0, 1, 2, 3],
            "letter": ["c", "b", "b", "a"],
            "num": [3, 1, 2, 1],
            "expected_keys": [3, 2, 1, 1],
            "expected_pairs_link": [9, 4, 1, 1],
            "expected_pairs_dedupe": [3, 1, 0, 0],
        }
    )
    c = linker.key_counts_left(inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_keys))
    assert c.n_total() == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)
    c = linker.key_counts_right(inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_keys))
    assert c.n_total() == 7
    # we need the CountsTable instance to be indistinguishable from a regular Table
    assert isinstance(c, ibis.Table)

    c = KeyLinker(keys, task="dedupe").pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_dedupe))
    c = KeyLinker(keys, task="link").pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_link))
    c = KeyLinker(keys).pair_counts(inp, inp)
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_dedupe))
    assert c.n_total() == 4

    # with a .view() table pair, check that dedupe/link behavior works
    c = KeyLinker(keys, task="link").pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_link))
    c = KeyLinker(keys, task="dedupe").pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_dedupe))
    c = KeyLinker(keys).pair_counts(inp, inp.view())
    assert_tables_equal(c, expected.select("letter", "num", n=_.expected_pairs_link))


def test_counts_empty(inp):
    # null keys will never get blocked with any others
    linker = KeyLinker(ibis.null(int))
    c = linker.pair_counts(inp, inp)
    assert c.count().execute() == 0
    assert c.n_total() == 0
