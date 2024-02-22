from __future__ import annotations

import warnings

import ibis
from ibis import _
from ibis.expr import types as it
import pytest

from mismo.block import SlowJoinError, SlowJoinWarning, block_one
from mismo.block._block import _resolve_predicates
from mismo.tests.util import assert_tables_equal

from .common import letter_blocked_ids


@pytest.mark.parametrize(
    "condition",
    [
        pytest.param("letter", id="string"),
        pytest.param(("letter", "letter"), id="tuple_strings"),
        pytest.param(
            lambda left, right, **_: left.letter == right.letter,
            id="lambda_bool_column",
        ),
        pytest.param(lambda left, right, **_: "letter", id="lambda_string"),
        pytest.param(
            lambda left, right, **_kwargs: (_.letter, _.letter), id="lambda_tuple"
        ),
        pytest.param((_.letter, _.letter), id="tuple_deferred"),
    ],
)
def test_block(table_factory, t1: it.Table, t2: it.Table, condition):
    blocked_table = block_one(t1, t2, condition)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = letter_blocked_ids(table_factory)
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


_TABLE = ibis.table(schema={"x": int})


def check_letter(t1, t2, resolved):
    assert len(resolved) == 1
    expected = t1.letter == t2.letter
    assert expected.equals(resolved[0])


def check_letter_array(t1, t2, resolved):
    assert len(resolved) == 2
    letter, array = resolved
    letter_expected = t1.letter == t2.letter
    array_expected = t1.array == t2.array
    assert letter_expected.equals(letter)
    assert array_expected.equals(array)


@pytest.mark.parametrize(
    "condition, expected",
    [
        pytest.param("letter", check_letter, id="single_str"),
        pytest.param(_.letter, check_letter, id="single_deferred"),
        pytest.param(("letter", "letter"), check_letter, id="pair_str"),
        pytest.param(("letter", _.letter), check_letter, id="pair_str_deferred"),
        pytest.param(["letter"], check_letter, id="list_single_str"),
        pytest.param([("letter", "letter")], check_letter, id="list_pair_str"),
        pytest.param([("letter", _.letter)], check_letter, id="list_pair_str_deferred"),
        pytest.param(
            ("letter", "array"),
            None,
            id="pair_letter_array",
            marks=pytest.mark.xfail(
                reason="strings and array<string> are not comparable"
            ),
        ),
        pytest.param(
            ("letter", _.array),
            None,
            id="pair_letter_array_deferred",
            marks=pytest.mark.xfail(
                reason="strings and array<string> are not comparable"
            ),
        ),
        pytest.param(
            [("letter", _.array)],
            None,
            id="list_pair_letter_array_deferred",
            marks=pytest.mark.xfail(
                reason="strings and array<string> are not comparable"
            ),
        ),
        pytest.param(
            ["letter", _.array],
            check_letter_array,
            id="list_letter_array_deferred",
        ),
        pytest.param(True, [True], id="true"),
        pytest.param([True], [True], id="true_list"),
        pytest.param(False, [False], id="false"),
        pytest.param([False], [False], id="false_list"),
        pytest.param(_TABLE, _TABLE, id="table"),
    ],
)
@pytest.mark.parametrize("wrap_in_lambda", [False, True])
def test_resolve_predicates(t1, t2, condition, expected, wrap_in_lambda):
    if wrap_in_lambda:
        resolved = _resolve_predicates(t1, t2, lambda left, right: condition)
    else:
        resolved = _resolve_predicates(t1, t2, condition)
    if callable(expected):
        expected(t1, t2, resolved)
    else:
        assert resolved == expected
