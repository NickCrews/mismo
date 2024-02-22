from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo._join import resolve_predicates

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
def test_resolve_condition(t1, t2, condition, expected, wrap_in_lambda):
    if wrap_in_lambda:
        resolved = resolve_predicates(t1, t2, lambda left, right: condition)
    else:
        resolved = resolve_predicates(t1, t2, condition)
    if callable(expected):
        expected(t1, t2, resolved)
    else:
        assert resolved == expected
