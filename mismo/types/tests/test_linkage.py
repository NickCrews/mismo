from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo import KeyLinkage, LinkedTable, LinkTableLinkage
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def linkage() -> LinkTableLinkage:
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9]})
    links = ibis.memtable(
        {"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9], "extra": [1, 2, 3]}
    )
    return LinkTableLinkage(left, right, links)


def test_LinkTableLinkage_init():
    left = ibis.memtable({"record_id": [4, 5, 6]})
    right = ibis.memtable({"record_id": [7, 8, 9]})
    links = ibis.memtable(
        {"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9], "extra": [1, 2, 3]}
    )
    # no error on extra column
    LinkTableLinkage(left, right, links)


@pytest.mark.xfail(reason="join keys such as (tl.foo, tr.bar) are not supported yet")
def test_KeyLinkage():
    tl = ibis.memtable({"foo": [1, 2, 3]})
    tr = ibis.memtable({"bar": [1, 2, 3]})
    linkage = KeyLinkage(tl, tr, (tl.foo, tr.bar))
    assert tuple(linkage.left.columns) == ("foo", "record_id")
    assert tuple(linkage.right.columns) == ("bar", "record_id")

    # Don't want test the actual contents, because the values
    # are just an implementation detail from using ibis.row_number().
    # They could be uuids or anything else.
    # So just test the len.
    assert set(linkage.links.columns) == {
        "record_id_l",
        "record_id_r",
        "foo_l",
        "bar_r",
    }
    assert linkage.links.count().execute() == 3


def test_LinkedTable_with_n_links(linkage: LinkTableLinkage):
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
            "foo": [1, 1, 1],
        }
    )
    assert_tables_equal(expected, actual)


def test_LinkedTable_link_counts(linkage: LinkTableLinkage):
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


def test_Linkage_link_counts_chart(linkage: LinkTableLinkage):
    # just a smoketest that it doesn't crash
    linkage.link_counts_chart()


def test_Linkage_empty_link_counts_chart(linkage: LinkTableLinkage):
    empty_linkage = LinkTableLinkage(
        left=linkage.left, right=linkage.right, links=linkage.links.limit(0)
    )
    empty_linkage.link_counts_chart()


def test_LinkTableLinkage_with_many_linked_values(
    linkage: LinkTableLinkage, table_factory
):
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


def test_LinkedTable_with_single_linked_values(
    linkage: LinkTableLinkage, table_factory
):
    result = linkage.left.with_single_linked_values(x="record_id", y=_.record_id + 1)
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (5, 9, 10),
        ],
        columns=["record_id", "x", "y"],
    )
    assert_tables_equal(expected, result)
