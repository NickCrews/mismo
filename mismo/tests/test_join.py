from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo._join import resolve_predicates

_TABLE = ibis.table(schema={"x": int})


def check_letters(t1, t2, resolved):
    assert len(resolved) == 1
    x, y = resolved[0]
    assert x.equals(t1.letters)
    assert y.equals(t2.letters)


def check_letters_arrays(t1, t2, resolved):
    assert len(resolved) == 1
    x, y = resolved[0]
    assert x.equals(t1.letters)
    assert y.equals(t2.arrays)


def check_letters_arrays2(t1, t2, resolved):
    assert len(resolved) == 2
    letters, arrays = resolved
    assert letters[0].equals(t1.letters)
    assert letters[1].equals(t2.letters)
    assert arrays[0].equals(t1.arrays)
    assert arrays[1].equals(t2.arrays)


@pytest.mark.parametrize(
    "condition, expected",
    [
        pytest.param("letters", check_letters, id="single_str"),
        pytest.param(_.letters, check_letters, id="single_deferred"),
        pytest.param(("letters", "letters"), check_letters, id="pair_str"),
        pytest.param(("letters", _.letters), check_letters, id="pair_str_deferred"),
        pytest.param(["letters"], check_letters, id="list_single_str"),
        pytest.param([("letters", "letters")], check_letters, id="list_pair_str"),
        pytest.param(
            [("letters", _.letters)], check_letters, id="list_pair_str_deferred"
        ),
        pytest.param(
            ("letters", "arrays"), check_letters_arrays, id="pair_letters_arrays"
        ),
        pytest.param(
            ("letters", _.arrays),
            check_letters_arrays,
            id="pair_letters_arrays_deferred",
        ),
        pytest.param(
            [("letters", _.arrays)],
            check_letters_arrays,
            id="list_pair_letters_arrays_deferred",
        ),
        pytest.param(
            ["letters", _.arrays],
            check_letters_arrays2,
            id="list_letters_arrays_deferred",
        ),
        pytest.param(True, [True], id="true"),
        pytest.param([True], [True], id="true_list"),
        pytest.param(False, [False], id="false"),
        pytest.param([False], [False], id="false_list"),
        pytest.param(_TABLE, [_TABLE], id="table"),
        pytest.param([_TABLE], [_TABLE], id="table"),
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
