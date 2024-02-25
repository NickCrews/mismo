from __future__ import annotations

import warnings

import ibis
from ibis import _
from ibis.expr import types as it
import pytest

import mismo
from mismo.block import SlowJoinError, SlowJoinWarning, block_one
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
def test_block(t1: it.Table, t2: it.Table, condition, expected_maker):
    blocked_table = block_one(t1, t2, condition)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = expected_maker(t1, t2)
    assert_tables_equal(blocked_ids, expected)


def test_cross_block(table_factory, t1: it.Table, t2: it.Table):
    blocked_table = block_one(t1, t2, True, on_slow="ignore")
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = table_factory(
        {
            "record_id_l": [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2],
            "record_id_r": [90, 90, 90, 91, 91, 91, 92, 92, 92, 93, 93, 93],
        }
    )
    assert_tables_equal(expected, blocked_ids)


def test_empty_block(t1: it.Table, t2: it.Table):
    blocked = block_one(t1, t2, False)
    assert "record_id_l" in blocked.columns
    assert "record_id_r" in blocked.columns
    n = blocked.count().execute()
    assert n == 0


def test_block_unnest(table_factory, t1: it.Table, t2: it.Table):
    # If you do a ibis.join(l, r, _.array.unnest()), that will fail because
    # you can't use unnest in a join condition.
    # But we want to support this case, so test our workaround.
    blocked_table = block_one(t1, t2, _.array.unnest())
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = table_factory({"record_id_l": [0, 1], "record_id_r": [90, 90]})
    assert_tables_equal(blocked_ids, expected)


def test_patents_unnest():
    t = mismo.datasets.load_patents().select("record_id", classes=_.classes.split("**"))
    b = block_one(t, t, _.classes.unnest())
    assert b.count().execute() == 569034


@pytest.mark.parametrize(
    "condition,is_slow",
    [
        pytest.param("letter", False, id="simple equijoin"),
        pytest.param(True, True, id="cross join"),
        pytest.param(
            lambda left, right, **_: left.letter.levenshtein(right.letter) < 2,
            True,
            id="levenshtein",
        ),
        pytest.param(
            lambda left, right, **_: (left.letter == right.letter)
            | (left.record_id == right.record_id),
            True,
            id="OR",
        ),
        pytest.param(
            lambda left, right, **_: (left.letter == right.letter)
            & (left.record_id == right.record_id),
            False,
            id="AND",
        ),
    ],
)
@pytest.mark.parametrize(
    "on_slow,result",
    [
        ("ignore", None),
        ("warn", SlowJoinWarning),
        ("error", SlowJoinError),
    ],
)
def test_warn_slow_join(
    t1: it.Table, t2: it.Table, condition, is_slow, on_slow, result
):
    def f():
        block_one(t1, t2, condition, on_slow=on_slow)

    if result is None:
        f()
    elif is_slow and result is SlowJoinWarning:
        with warnings.catch_warnings(record=True) as w:
            f()
            assert len(w) == 1
            assert issubclass(w[0].category, SlowJoinWarning)
    elif is_slow and result is SlowJoinError:
        with pytest.raises(SlowJoinError):
            f()
