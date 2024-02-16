from __future__ import annotations

import warnings

from ibis import _
from ibis.expr.types import Table
import pytest

from mismo.block import SlowJoinError, SlowJoinWarning, block
from mismo.tests.util import assert_tables_equal

from .common import letters_blocked_ids


@pytest.mark.parametrize(
    "condition",
    [
        pytest.param("letters", id="string"),
        pytest.param(("letters", "letters"), id="tuple_strings"),
        pytest.param(
            lambda left, right, **_: left.letters == right.letters,
            id="lambda_bool_column",
        ),
        pytest.param(lambda left, right, **_: "letters", id="lambda_string"),
        pytest.param(
            lambda left, right, **_kwargs: (_.letters, _.letters), id="lambda_tuple"
        ),
        pytest.param((_.letters, _.letters), id="tuple_deferred"),
        pytest.param([(_.letters, _.letters)], id="list_tuple_deferred"),
    ],
)
def test_block(table_factory, t1: Table, t2: Table, condition):
    blocked_table = block(t1, t2, condition)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = letters_blocked_ids(table_factory)
    assert_tables_equal(blocked_ids, expected)


def test_cross_block(table_factory, t1: Table, t2: Table):
    blocked_table = block(t1, t2, True, on_slow="ignore")
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
        pytest.param("letters", False, id="simple equijoin"),
        pytest.param(True, True, id="cross join"),
        pytest.param(
            lambda left, right, **_: left.letters.levenshtein(right.letters) < 2,
            True,
            id="levenshtein",
        ),
        pytest.param(
            lambda left, right, **_: (left.letters == right.letters)
            | (left.record_id == right.record_id),
            True,
            id="OR",
        ),
        pytest.param(
            lambda left, right, **_: (left.letters == right.letters)
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
def test_warn_slow_join(t1: Table, t2: Table, condition, is_slow, on_slow, result):
    def f():
        block(t1, t2, condition, on_slow=on_slow)

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
