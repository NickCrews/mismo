from __future__ import annotations

from ibis import _
from ibis.expr.types import Table
import pytest

from mismo.block import BlockingRule, block
from mismo.tests.util import assert_tables_equal

from .common import letters_blocked


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
def test_blocking_rule(table_factory, t1: Table, t2: Table, condition):
    name = "test_rule"
    rule = BlockingRule(condition, name=name)
    assert rule.get_name() == name
    assert rule.condition == condition
    expected = letters_blocked(table_factory)
    assert_tables_equal(expected, rule.block(t1, t2))
    assert_tables_equal(expected, block(t1, t2, rule))
