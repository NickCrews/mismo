from __future__ import annotations

import ibis
import pytest

from mismo.linker import LabelLinker
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def a(table_factory):
    return table_factory(
        {
            "record_id": [1, 2, 3, 4],
            "label": [9, 9, None, 8],
        }
    )


@pytest.fixture
def b(table_factory):
    return table_factory(
        {
            "record_id": [11, 12, 13, 14],
            "label": [7, None, 8, 8],
        }
    )


def test_indefinite_join_condition(table_factory, a, b):
    ll = LabelLinker("label")
    condition = ll.indefinite_join_condition(a, b)
    actual = (
        ibis.join(a, b, condition, lname="{name}_l", rname="{name}_r")
        .select("record_id_l", "record_id_r")
        .distinct()
    )
    expected = table_factory(
        {
            "record_id_l": [3, 3, 3, 3, 1, 2, 4],
            "record_id_r": [11, 12, 13, 14, 12, 12, 12],
        }
    )
    assert_tables_equal(expected, actual)
