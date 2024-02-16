from __future__ import annotations

import ibis
import pandas._testing as tm
import pytest

from mismo.lib import name


@pytest.mark.parametrize(
    ("name1", "name2", "expected"),
    [
        ("alex", "alex", True),
        (" ALEX ", "alex", True),
        ("al", "alex", True),
        ("alex", "al", True),
        ("alex", "alexander", True),
        ("al", "alexander", True),
        ("alexa", "alexander", False),
        ("zach", "zack", True),
        ("betsy", "jon", False),
        ("mary", "mary", True),
        ("bob", "bob", True),
        ("robert", "bob", True),
        ("bob", "robert", True),
        ("bob", "  ROBERT", True),
        ("robert", "mary", False),
        ("robert", "robert", True),
        ("robert", "roberta", False),
        (ibis.literal("robert"), "bob", True),
        (ibis.literal("bob"), "bob", True),
    ],
)
def test_are_aliases(name1, name2, expected):
    result = name.are_aliases(name1, name2)
    if isinstance(result, ibis.Expr):
        result = result.execute()
    assert expected == result


def test_are_aliases_column():
    t = ibis.memtable(
        {
            "a": ["robert", "bob", "robert", "rob"],
            "b": ["mary", "robert", "rob", "rob"],
            "expected": [False, True, True, True],
        }
    )
    res = name.are_aliases(t.a, t.b)
    print(id(res._find_backend(use_default=True)))
    result = t.mutate(aliases=res).execute()
    tm.assert_series_equal(result.expected, result.aliases, check_names=False)
