from __future__ import annotations

from ibis import _
from ibis.expr import types as it
import pytest

from mismo.block import BlockingRule, block
from mismo.tests.util import assert_tables_equal

from .common import letters_blocked_ids


@pytest.mark.parametrize(
    "condition",
    [
        "letters",
        ("letters", "letters"),
        lambda left, right, **kwargs: left.letters == right.letters,
        lambda left, right, **kwargs: "letters",
        lambda left, right, **kwargs: (_.letters, _.letters),
        (_.letters, _.letters),
        [(_.letters, _.letters)],
    ],
)
def test_blocking_rule(table_factory, t1: it.Table, t2: it.Table, condition):
    name = "test_rule"
    rule = BlockingRule(condition, name=name)
    assert rule.get_name() == name
    assert rule.condition == condition
    expected = letters_blocked_ids(table_factory)
    assert_tables_equal(expected, rule.block(t1, t2)["record_id_l", "record_id_r"])
    assert_tables_equal(expected, block(t1, t2, rule)["record_id_l", "record_id_r"])
