from __future__ import annotations

from ibis.expr import types as ir
import pytest

import mismo


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
        ("warn", mismo.exceptions.SlowJoinWarning),
        ("error", mismo.exceptions.SlowJoinError),
    ],
)
def test_warn_slow_join(
    t1: ir.Table, t2: ir.Table, condition, is_slow, on_slow, result
):
    def f():
        mismo.linker.JoinLinker(condition, on_slow=on_slow)(t1, t2)

    if result is None:
        f()
    elif is_slow and result is mismo.exceptions.SlowJoinWarning:
        with pytest.warns(mismo.exceptions.SlowJoinWarning):
            f()
    elif is_slow and result is mismo.exceptions.SlowJoinError:
        with pytest.raises(mismo.exceptions.SlowJoinError):
            f()
