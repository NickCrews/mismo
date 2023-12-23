from __future__ import annotations

from ibis import _
from ibis.expr.types import Table
import pytest

from mismo.block import BlockingRule
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def left_table(table_factory) -> Table:
    return table_factory({"record_id": [0, 1, 2], "letters": ["a", "b", "c"]})


@pytest.fixture
def right_table(table_factory) -> Table:
    return table_factory({"record_id": [90, 91, 92], "letters": ["b", "c", "d"]})


@pytest.mark.parametrize(
    "condition",
    [
        "letters",
        ("letters", "letters"),
        lambda left, right: left.letters == right.letters,
        lambda left, right: "letters",
        lambda left, right: (_.letters, _.letters),
        (_.letters, _.letters),
        [(_.letters, _.letters)],
    ],
)
def test_blocking_rule_condition(
    table_factory, left_table: Table, right_table: Table, condition
):
    name = "test_rule"
    rule = BlockingRule(name, condition)
    assert rule.name == name
    assert rule.condition == condition
    blocked_table = rule.block(left_table, right_table)
    expected = table_factory(
        {
            "record_id_l": [1, 2],
            "record_id_r": [90, 91],
            "letters_l": ["b", "c"],
            "letters_r": ["b", "c"],
        }
    )
    assert_tables_equal(blocked_table, expected)


def test_cross_block(table_factory, left_table: Table, right_table: Table):
    name = "test_rule"
    rule = BlockingRule(name, True)
    blocked_table = rule.block(left_table, right_table)
    expected = table_factory(
        {
            "record_id_l": [0, 1, 2, 0, 1, 2, 0, 1, 2],
            "record_id_r": [90, 90, 90, 91, 91, 91, 92, 92, 92],
            "letters_l": ["a", "b", "c", "a", "b", "c", "a", "b", "c"],
            "letters_r": ["b", "b", "b", "c", "c", "c", "d", "d", "d"],
        }
    )
    assert_tables_equal(blocked_table, expected)
