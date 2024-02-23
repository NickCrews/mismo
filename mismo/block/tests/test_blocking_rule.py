from __future__ import annotations

from ibis import _
from ibis.expr import types as it
import pytest

from mismo.block import BlockingRule, block_one
from mismo.tests.util import assert_tables_equal

from .common import letter_blocked_ids


@pytest.mark.parametrize(
    "condition,name,expected_name",
    [
        ("letter", None, "letter"),
        (_.letter, None, "_.letter"),
        ("letter", "my_name", "my_name"),
    ],
)
def test_blocking_rule(
    table_factory, t1: it.Table, t2: it.Table, condition, name, expected_name
):
    rule = BlockingRule(condition, name=name)
    assert rule.get_name() == expected_name
    assert rule.condition is condition
    expected = letter_blocked_ids(table_factory)
    assert_tables_equal(expected, block_one(t1, t2, rule)["record_id_l", "record_id_r"])
