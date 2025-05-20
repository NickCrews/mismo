from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo import Linkage, LinkedTable
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def linkage() -> Linkage:
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9], "extra": ["a", "b", "c"]})
    links = ibis.memtable(
        {"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9], "extra": [1, 2, 3]}
    )
    return Linkage(left=left, right=right, links=links)


def test_Linkage_init():
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9]})
    links = ibis.memtable(
        {"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9], "extra": [1, 2, 3]}
    )
    # no error on extra column
    Linkage(left=left, right=right, links=links)


def test_LinkedTable_with_n_links(linkage: Linkage):
    actual = linkage.left.with_n_links()
    expected = ibis.memtable(
        {
            "record_id": [4, 5, 6],
            "n_links": [2, 1, 0],
        }
    )
    assert_tables_equal(expected, actual)

    actual = linkage.right.with_n_links(name="foo")
    expected = ibis.memtable(
        {
            "record_id": [7, 8, 9],
            "extra": ["a", "b", "c"],
            "foo": [1, 1, 1],
        }
    )
    assert_tables_equal(expected, actual)


def test_LinkedTable_link_counts(linkage: Linkage):
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
    assert_tables_equal(expected, actual, column_order="ignore")

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
    assert_tables_equal(expected, actual, column_order="ignore")


def test_Linkage_link_counts_chart(linkage: Linkage):
    # just a smoketest that it doesn't crash
    linkage.link_counts_chart()


def test_Linkage_empty_link_counts_chart(linkage: Linkage):
    empty_linkage = Linkage(
        left=linkage.left, right=linkage.right, links=linkage.links.limit(0)
    )
    empty_linkage.link_counts_chart()


def test_Linkage_with_linked_values(linkage: Linkage, table_factory):
    result = linkage.left.with_linked_values(
        "extra",
        x=_.record_id.max(),
        y=(_.record_id + 1),
    )
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (4, ["a", "b"], 8, [8, 9]),
            (5, ["c"], 9, [10]),
            (6, None, None, None),
        ],
        schema={
            "record_id": "int64",
            "extra": "array<string>",
            "x": "int64",
            "y": "array<int64>",
        },
    )
    assert_tables_equal(expected, result)
