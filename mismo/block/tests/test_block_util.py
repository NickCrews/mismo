from __future__ import annotations

from ibis import _
import pytest

from mismo.block import _util


@pytest.mark.parametrize(
    "x,expected",
    [
        pytest.param("a", "a", id="str"),
        pytest.param(("a", "b"), "(a, b)", id="str_tuple"),
        pytest.param(_.a, "_.a", id="deferred"),
        pytest.param((_.a, _.b), "(_.a, _.b)", id="deferred_tuple"),
        pytest.param((_.a + 2, _.b), "((_.a + 2), _.b)", id="deffered +"),
        pytest.param([_.a, "b"], "[_.a, b]", id="list"),
        pytest.param([(_.a, "a"), "b"], "[(_.a, a), b]", id="list with tuple"),
    ],
)
def test_blocker_name(x, expected):
    result = _util.blocker_name(x)
    assert expected == result
