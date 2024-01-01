from __future__ import annotations

from ibis import _
from ibis.expr.types import Table
import pytest

from mismo.compare import Comparison, ComparisonLevel, Comparisons


@pytest.fixture()
def blocked(table_factory) -> Table:
    return table_factory(
        {
            "cost_l": [1, 1, 99],
            "cost_r": [1, 2, 99],
            "cost_name": ["exact", "else", "large"],
            "cost_index": [1, 2, 0],
            "tag_l": ["a", "b", "c"],
            "tag_r": ["A", "d", None],
            "tag_name": ["same ignore case", "else", "else"],
            "tag_index": [1, 2, 2],
        }
    )


@pytest.mark.parametrize(
    "condition, expected_true_count",
    [
        (_.cost_r == 1, 1),
        (_.cost_l == _.cost_r, 2),
        (True, 3),
        (False, 0),
        (lambda _: True, 3),
        (lambda t: t.cost_l == t.cost_r, 2),
    ],
)
def test_comparison_level_conditions(condition, expected_true_count, blocked):
    level = ComparisonLevel("foo", condition)
    assert level.name == "foo"
    blocked = blocked.mutate(is_match=level.is_match(blocked))
    assert blocked.is_match.sum().execute() == expected_true_count


@pytest.fixture
def large_level():
    return ComparisonLevel("large", _.cost_l > 10)


@pytest.fixture
def exact_level():
    return ComparisonLevel("exact", _.cost_l == _.cost_r)


def test_comparison_without_else_level(large_level, exact_level):
    """You can construct a Comparison *without* an ELSE level."""
    comp = Comparison("cost", [large_level, exact_level])
    assert comp.name == "cost"
    assert len(comp) == 3
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["else"].name == "else"


def test_comparison_with_else_level(large_level, exact_level):
    """You can construct a Comparison *with* an ELSE level."""
    comp = Comparison("cost", [large_level, exact_level, ComparisonLevel("else", True)])
    assert comp.name == "cost"
    assert len(comp) == 3
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["else"].name == "else"


def test_comparison_else_case_sensitive(large_level, exact_level):
    """The ELSE level should be case sensitive"""
    comp = Comparison(
        "cost",
        [
            large_level,
            exact_level,
            ComparisonLevel("ELSE", True),
            ComparisonLevel("else", True),
        ],
    )
    assert comp.name == "cost"
    assert len(comp) == 4
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["ELSE"].name == "ELSE"
    assert comp["else"].name == "else"


def test_comparison_else_level_not_last(large_level, exact_level):
    """If else level is not last, it should error."""
    with pytest.raises(ValueError):
        Comparison("cost", [large_level, ComparisonLevel("else", True), exact_level])


@pytest.fixture
def cost_comparison(large_level, exact_level):
    return Comparison("cost", [large_level, exact_level])


def test_comparison_basic(cost_comparison, large_level, exact_level):
    assert cost_comparison.name == "cost"
    assert len(cost_comparison) == 3
    assert cost_comparison["large"] == large_level
    assert cost_comparison["exact"] == exact_level
    assert cost_comparison["else"].name == "else"
    assert cost_comparison[0] == large_level
    assert cost_comparison[1] == exact_level
    assert cost_comparison[2].name == "else"

    levels = [level for level in cost_comparison]
    assert levels[0] == large_level
    assert levels[1] == exact_level
    assert levels[2].name == "else"

    with pytest.raises(KeyError):
        cost_comparison["foo"]
    with pytest.raises(IndexError):
        cost_comparison[3]


@pytest.mark.parametrize(
    "how, expected_col",
    [
        ("name", "cost_name"),
        ("index", "cost_index"),
    ],
)
def test_comparison_label(blocked: Table, cost_comparison, how, expected_col):
    t = blocked.mutate(label=cost_comparison.label_pairs(blocked, how=how))
    assert (t.label == t[expected_col]).all().execute()


@pytest.fixture
def tag_comparison():
    return Comparison(
        "tag",
        [
            ComparisonLevel("exact", _.tag_l == _.tag_r),
            ComparisonLevel("same ignore case", _.tag_l.lower() == _.tag_r.lower()),
        ],
    )


@pytest.fixture
def comparisons(cost_comparison, tag_comparison):
    return Comparisons(cost_comparison, tag_comparison)


def test_comparisons_basic(comparisons, cost_comparison, tag_comparison):
    assert len(comparisons) == 2
    assert comparisons["cost"] == cost_comparison
    assert comparisons["tag"] == tag_comparison

    assert set(comparisons) == {cost_comparison, tag_comparison}

    with pytest.raises(KeyError):
        comparisons["foo"]
    with pytest.raises(KeyError):
        comparisons[0]


def test_comparisons_label(comparisons: Comparisons, blocked):
    t = comparisons.label_pairs(blocked)
    assert set(t.columns) == set(blocked.columns) | {"cost", "tag"}
    assert (t.cost == t.cost_index).all().execute()
    assert (t.tag == t.tag_index).all().execute()


@pytest.mark.xfail(reason="ibis bug? Need to check")
def test_setwise_comparison(table_factory):
    d = {
        "names_l": [["a", "b"], ["c", "d"], [], None],
        "names_r": [["b", "x"], ["y", "z"], ["m"], []],
        "expected": [True, False, False, False],
    }
    t = table_factory(d)
    c = Comparison(
        "names",
        [
            ComparisonLevel(
                "any_equal",
                # could do the same with ibis's Array.intersect,
                # but I want to check this works with a custom function
                _array_any(
                    _.names_l.map(
                        lambda left: _array_any(
                            _.names_r.map(lambda right: left == right)
                        )
                    )
                ),
            ),
        ],
    )
    t = t.mutate(result=c.label_pairs(t))
    assert (t.result == t.expected).all().execute()


def _array_any(x):
    return x.filter(lambda x: x).length() > 0
