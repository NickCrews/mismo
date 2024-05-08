from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir
import pytest

import mismo
from mismo.block import KeyBlocker
from mismo.tests.util import assert_tables_equal


def blocked_on_letter(t1, t2, **kwargs):
    return ibis.join(
        t1, t2, t1.letter == t2.letter, lname="{name}_l", rname="{name}_r"
    )["record_id_l", "record_id_r"]


def blocked_on_letter_int(t1, t2, **kwargs):
    return ibis.join(
        t1,
        t2,
        (t1.letter == t2.letter) & (t1.int == t2.int),
        lname="{name}_l",
        rname="{name}_r",
    )["record_id_l", "record_id_r"]


def letter_blocked_ids(table_factory):
    """If you block the fixtures t1 and t2 on the letter column,
    these are the record_ids what you should get."""
    return table_factory(
        {
            "record_id_l": [1, 2],
            "record_id_r": [90, 91],
        }
    )


@pytest.mark.parametrize(
    "condition,name,expected_name",
    [
        ("letter", None, "letter"),
        (_.letter, None, "_.letter"),
        ("letter", "my_name", "my_name"),
    ],
)
def test_blocking_rule(
    table_factory, t1: ir.Table, t2: ir.Table, condition, name, expected_name
):
    rule = KeyBlocker(condition, name=name)
    assert rule.name == expected_name
    expected = letter_blocked_ids(table_factory)
    assert_tables_equal(expected, rule(t1, t2)["record_id_l", "record_id_r"])


@pytest.mark.parametrize(
    "condition,expected_maker",
    [
        pytest.param("letter", blocked_on_letter, id="letter"),
        pytest.param(("letter", "int"), blocked_on_letter_int, id="letter_int"),
        pytest.param((_.letter, _.int + 1), blocked_on_letter_int, id="tuple_deferred"),
        pytest.param(
            lambda left, right, **_: left.letter == right.letter,
            blocked_on_letter,
            id="lambda_bool_column",
        ),
        pytest.param(
            lambda left, right, **_: "letter", blocked_on_letter, id="lambda_letter"
        ),
        pytest.param(
            lambda left, right, **_kwargs: (_.letter, _.int + 1),
            blocked_on_letter_int,
            id="lambda_tuple",
        ),
        pytest.param(
            blocked_on_letter, blocked_on_letter, id="callable_returning_table"
        ),
    ],
)
def test_block(t1: ir.Table, t2: ir.Table, condition, expected_maker):
    blocked_table = KeyBlocker(condition)(t1, t2)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = expected_maker(t1, t2)
    assert_tables_equal(blocked_ids, expected)


def test_unnest(table_factory, t1: ir.Table, t2: ir.Table):
    # If you do a ibis.join(l, r, _.array.unnest()), that will fail because
    # you can't use unnest in a join condition.
    # But we want to support this case, so test our workaround.
    blocked_table = KeyBlocker(_.array.unnest())(t1, t2)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = table_factory({"record_id_l": [0, 1], "record_id_r": [90, 90]})
    assert_tables_equal(blocked_ids, expected)


def test_patents_unnest():
    t = mismo.datasets.load_patents().select("record_id", classes=_.classes.split("**"))
    b = KeyBlocker(_.classes.unnest())(t, t)
    assert b.count().execute() == 569034
