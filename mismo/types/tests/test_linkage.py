from __future__ import annotations

import re

import ibis
from ibis import _
import pytest

from mismo.tests.util import assert_tables_equal
from mismo.types import Linkage, LinkedTable

# eg id_l_6K9WXYNTN0ZK0OXFCLR3M3Q6S
default_id_pattern = re.compile(r"\w{20,30}")


@pytest.fixture
def linkage() -> Linkage:
    left = ibis.memtable({"idl": [4, 5, 6]})
    right = ibis.memtable({"idr": [7, 8, 9]})
    links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
    return Linkage(left, right, links)


def test_Linkage_init():
    left = ibis.memtable({"idl": [4, 5, 6]})
    right = ibis.memtable({"idr": [7, 8, 9]})
    links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9], "extra": [1, 2, 3]})
    # no error on extra column
    Linkage(left, right, links)


def test_Linkage_from_predicates():
    tl = ibis.memtable({"x": [1, 2, 3]})
    tr = ibis.memtable({"x": [1, 2, 2]})
    linkage = Linkage.from_predicates(tl, tr, "x")
    assert default_id_pattern.match(linkage.left_id)
    assert default_id_pattern.match(linkage.right_id)
    assert linkage.left_id != linkage.right_id
    assert len(linkage.links.columns) == 2
    assert linkage.links.columns[0] == linkage.left_id
    assert linkage.links.columns[1] == linkage.right_id
    assert tuple(linkage.left.columns) == ("x", linkage.left_id)
    assert tuple(linkage.right.columns) == ("x", linkage.right_id)

    # Currently, links looks like this:
    # ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    # ┃ record_id_38HRXB3MYA5SM20LHX7GP8R4V ┃ record_id_77IJWQ0DV0QRAF5JCQIB5VAIE ┃
    # ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    # │ int64                               │ int64                               │
    # ├─────────────────────────────────────┼─────────────────────────────────────┤
    # │                                   0 │                                   0 │
    # │                                   1 │                                   2 │
    # │                                   1 │                                   1 │
    # └─────────────────────────────────────┴─────────────────────────────────────┘
    # but I don't want to test the actual contents, because the values
    # are just an implementation detail from using ibis.row_number()
    # So just test the len
    assert linkage.links.count().execute() == 3


def test_LinkedTable_with_n_links():
    left = ibis.memtable({"idl": [4, 5, 6]})
    right = ibis.memtable({"idr": [7, 8, 9]})
    links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
    linkage = Linkage(left, right, links)

    assert tuple(linkage.left.with_n_links("foo").columns) == ("idl", "foo")

    actual = linkage.left.with_n_links()
    expected = ibis.memtable(
        {
            "idl": [4, 5, 6],
            "n_links": [2, 1, 0],
        }
    )
    assert_tables_equal(expected, actual)

    actual = linkage.right.with_n_links()
    expected = ibis.memtable(
        {
            "idr": [7, 8, 9],
            "n_links": [1, 1, 1],
        }
    )
    assert_tables_equal(expected, actual)


def test_LinkedTable_link_counts():
    tl = ibis.memtable({"x": [1, 2, 3]})
    tr = ibis.memtable({"x": [1, 2, 2]})
    linkage = Linkage.from_predicates(tl, tr, "x")
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
    result = linkage.left.with_many_linked_values("idr", y=_.idr + 1)
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (4, [7, 8], [8, 9]),
            (5, [9], [10]),
            (6, [], []),
        ],
        columns=["idl", "idr", "y"],
    )
    assert_tables_equal(expected, result)


def test_LinkedTable_with_single_linked_values(linkage: Linkage, table_factory):
    result = linkage.left.with_single_linked_values("idr", y=_.idr + 1)
    assert isinstance(result, LinkedTable)

    expected = table_factory(
        [
            (5, 9, 10),
        ],
        columns=["idl", "idr", "y"],
    )
    assert_tables_equal(expected, result)
