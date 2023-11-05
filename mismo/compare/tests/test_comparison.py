from __future__ import annotations

import ibis
from ibis import _
import pandas as pd
import pytest

from mismo.compare import Comparison, ComparisonLevel


@pytest.fixture()
def blocked_df():
    return pd.DataFrame(
        {
            "cost_l": [1, 1, 99],
            "cost_r": [1, 2, 99],
        }
    )


@pytest.mark.parametrize(
    "condition, expected",
    [
        (True, [True, True, True]),
        (False, [False, False, False]),
        (_.cost_l == _.cost_r, [True, False, True]),
        (lambda _: True, [True, True, True]),
        (lambda t: t.cost_l == t.cost_r, [True, False, True]),
    ],
)
def test_comparison_level_conditions(condition, expected, blocked_df):
    level = ComparisonLevel("foo", condition)
    assert level.name == "foo"
    assert level.description is None
    t = ibis.memtable(blocked_df.assign(expected=expected))
    t = t.mutate(matched=level.is_match(t))
    assert (t.matched == t.expected).all().execute()


@pytest.fixture
def large_level():
    return ComparisonLevel("large", _.cost_l > 10)


@pytest.fixture
def exact_level():
    return ComparisonLevel("exact", _.cost_l == _.cost_r)


@pytest.fixture
def comparison(large_level, exact_level):
    return Comparison("cost", [large_level, exact_level])


def test_comparison_basic(comparison, large_level, exact_level):
    assert comparison.name == "cost"
    assert len(comparison) == 3
    assert comparison["large"] == large_level
    assert comparison["exact"] == exact_level
    assert comparison["else"].name == "else"
    assert comparison[0] == large_level
    assert comparison[1] == exact_level
    assert comparison[2].name == "else"

    levels = [level for level in comparison]
    assert levels[0] == large_level
    assert levels[1] == exact_level
    assert levels[2].name == "else"


def test_comparison_label(blocked_df, comparison, large_level, exact_level):
    t = ibis.memtable(blocked_df.assign(expected=["exact", "else", "large"]))
    t = t.mutate(label=comparison.label_pairs(t, how="name"))
    assert (t.label == t.expected).all().execute()
