from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo.tests.util import assert_tables_equal
from mismo.types import Linkage, LinkedTable


@pytest.fixture
def linkage() -> Linkage:
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9]})
    links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
    return Linkage(left, right, links)


def test_Linkage_init():
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9]})
    links = ibis.memtable(
        {"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9], "extra": [1, 2, 3]}
    )
    # no error on extra column
    Linkage(left, right, links)


def test_Linkage_from_predicates():
    tl = ibis.memtable({"foo": [1, 2, 3]})
    tr = ibis.memtable({"bar": [1, 2, 3]})
    linkage = Linkage.from_predicates(tl, tr, tl.foo == tr.bar)
    assert tuple(linkage.left.columns) == ("foo", "record_id")
    assert tuple(linkage.right.columns) == ("bar", "record_id")

    # Currently, links looks like this:
    # ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
    # ┃ record_id_l ┃ record_id_r ┃
    # ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
    # │ int64       │ int64       │
    # ├─────────────┼─────────────┤
    # │           0 │           0 │
    # │           1 │           2 │
    # │           1 │           1 │
    # └─────────────┴─────────────┘
    # but I don't want to test the actual contents, because the values
    # are just an implementation detail from using ibis.row_number().
    # They could be uuids or anything else.
    # So just test the len
    assert tuple(linkage.links.columns) == ("record_id_l", "record_id_r")
    assert linkage.links.count().execute() == 3


def test_LinkedTable_with_n_links(linkage):
    assert tuple(linkage.left.with_n_links("foo").columns) == ("record_id", "foo")

    actual = linkage.left.with_n_links()
    expected = ibis.memtable(
        {
            "record_id": [4, 5, 6],
            "n_links": [2, 1, 0],
        }
    )
    assert_tables_equal(expected, actual)

    actual = linkage.right.with_n_links()
    expected = ibis.memtable(
        {
            "record_id": [7, 8, 9],
            "n_links": [1, 1, 1],
        }
    )
    assert_tables_equal(expected, actual)


def test_LinkedTable_link_counts(linkage):
    actual = linkage.left.link_counts()
    # ┏━━━━━━━━━┳━━━━━━━━━━━┓
    # ┃ n_links ┃ n_records ┃
    # ┡━━━━━━━━━╇━━━━━━━━━━━┩
    # │ int64   │ int64     │
    # ├─────────┼───────────┤
    # │       2 │         1 │  # there was 1 record in tl that matched 2 in tr
    # │       0 │         1 │  # there was 1 record in tl that didn't match any in tr
    # │       1 │         1 │  # there was 1 record in tl that matched 1 in tr
    # └─────────┴───────────┘
    expected = ibis.memtable(
        {
            "n_records": [1, 1, 1],
            "n_links": [0, 1, 2],
        }
    )
    assert_tables_equal(expected, actual)

    actual = linkage.right.link_counts()
    # ┏━━━━━━━━━┳━━━━━━━━━━━┓
    # ┃ n_links ┃ n_records ┃
    # ┡━━━━━━━━━╇━━━━━━━━━━━┩
    # │ int64   │ int64     │
    # ├─────────┼───────────┤
    # │       1 │         3 │  # there were 3 records in tr that matched 1 in tl
    # └─────────┴───────────┘
    expected = ibis.memtable(
        {
            "n_records": [3],
            "n_links": [1],
        }
    )
    assert_tables_equal(expected, actual)


def test_Linkage_link_counts_chart(linkage: Linkage):
    # just a smoketest that it doesn't crash
    linkage.link_counts_chart()


def test_LinkedTable_with_many_linked_values(linkage: Linkage, table_factory):
    result = linkage.left.with_many_linked_values(x="record_id", y=_.record_id + 1)
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (4, [7, 8], [8, 9]),
            (5, [9], [10]),
            (6, [], []),
        ],
        columns=["record_id", "x", "y"],
    )
    assert_tables_equal(expected, result)


def test_LinkedTable_with_single_linked_values(linkage: Linkage, table_factory):
    result = linkage.left.with_single_linked_values(x="record_id", y=_.record_id + 1)
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (5, 9, 10),
        ],
        columns=["record_id", "x", "y"],
    )
    assert_tables_equal(expected, result)
