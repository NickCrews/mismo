from __future__ import annotations

from ibis import _
from ibis.expr.types import Table
import pytest

from mismo.compare import LevelComparer, compare


@pytest.fixture()
def blocked(table_factory) -> Table:
    return table_factory(
        {
            "cost_l": [1, 1, 99],
            "cost_r": [1, 2, 99],
            "cost_label_expected": ["exact", "else", "large"],
            "tag_l": ["a", "b", "c"],
            "tag_r": ["A", "d", None],
            "tag_label_expected": ["same ignore case", "else", "else"],
        }
    )


def test_array_based_conditions(table_factory):
    t = table_factory(
        {
            "tags_l": [["a", "b"], ["c", "d"], [], None],
            "tags_r": [["b", "x"], ["y", "z"], ["m"], []],
            "expected": ["any_equal", "else", "else", "else"],
        }
    )

    def any_(arr):
        # Because Ibis doesn't have an ArrayValue.any() method we need this function
        # https://github.com/ibis-project/ibis/issues/7073
        return arr.filter(lambda x: x).length() > 0

    level = dict(
        name="any_equal",
        # We could also do this with ibis's Array.intersect, but I want to check
        # this more low-level approach works.
        condition=lambda t: any_(
            t.tags_l.map(lambda ltag: any_(t.tags_r.map(lambda rtag: ltag == rtag)))
        ),
    )
    t = compare(t, LevelComparer("intersection", [level]))
    assert (t.intersection == t.expected).all().execute()


@pytest.fixture
def large_level():
    return dict(name="large", condition=_.cost_l > 10)


@pytest.fixture
def exact_level():
    return dict(name="exact", condition=_.cost_l == _.cost_r)


def test_comparison_without_else_level(large_level, exact_level):
    """You can construct a Comparison *without* an ELSE level."""
    comp = LevelComparer("cost", [large_level, exact_level])
    assert comp.name == "cost"
    assert len(comp) == 3
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["else"]["name"] == "else"


def test_comparison_with_else_level(large_level, exact_level):
    """You can construct a Comparison *with* an ELSE level."""
    comp = LevelComparer(
        "cost", [large_level, exact_level, dict(name="else", condition=True)]
    )
    assert comp.name == "cost"
    assert len(comp) == 3
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["else"]["name"] == "else"


def test_comparison_else_case_sensitive(large_level, exact_level):
    """The ELSE level should be case sensitive"""
    comp = LevelComparer(
        "cost",
        [
            large_level,
            exact_level,
            dict(name="ELSE", condition=True),
            dict(name="else", condition=True),
        ],
    )
    assert comp.name == "cost"
    assert len(comp) == 4
    assert comp["large"] == large_level
    assert comp["exact"] == exact_level
    assert comp["ELSE"]["name"] == "ELSE"
    assert comp["else"]["name"] == "else"


def test_else_level_not_last(large_level, exact_level):
    """If else level is not last, it should error."""
    with pytest.raises(ValueError):
        LevelComparer(
            "cost", [large_level, dict(name="else", condition=True), exact_level]
        )


@pytest.fixture
def cost_comparer(large_level, exact_level) -> LevelComparer:
    return LevelComparer("cost", [large_level, exact_level])


def test_comparer_basic(cost_comparer: LevelComparer, large_level, exact_level):
    assert cost_comparer.name == "cost"
    assert len(cost_comparer) == 3
    assert cost_comparer["large"] == large_level
    assert cost_comparer["exact"] == exact_level
    assert cost_comparer["else"]["name"] == "else"
    assert cost_comparer[0] == large_level
    assert cost_comparer[1] == exact_level
    assert cost_comparer[2]["name"] == "else"

    levels = [level for level in cost_comparer]
    assert levels[0] == large_level
    assert levels[1] == exact_level
    assert levels[2]["name"] == "else"

    with pytest.raises(KeyError):
        cost_comparer["foo"]
    with pytest.raises(IndexError):
        cost_comparer[3]


def test_comparison_label(blocked: Table, cost_comparer):
    t = compare(blocked, cost_comparer)
    assert (t.cost == t.cost_label_expected).all().execute()


@pytest.fixture
def tag_comparer():
    return LevelComparer(
        "tag",
        [
            dict(name="exact", condition=_.tag_l == _.tag_r),
            dict(name="same ignore case", condition=_.tag_l.lower() == _.tag_r.lower()),
        ],
    )


@pytest.fixture
def comparers(cost_comparer, tag_comparer):
    return [cost_comparer, tag_comparer]


def test_comparisons_label(comparers: list[LevelComparer], blocked):
    t = compare(blocked, *comparers)
    assert set(t.columns) == set(blocked.columns) | {"cost", "tag"}
    assert (t.cost == t.cost_label_expected).all().execute()
    assert (t.tag == t.tag_label_expected).all().execute()


@pytest.mark.xfail(reason="ibis bug? Need to check")
def test_setwise_comparison(table_factory):
    d = {
        "names_l": [["a", "b"], ["c", "d"], [], None],
        "names_r": [["b", "x"], ["y", "z"], ["m"], []],
        "expected": [True, False, False, False],
    }
    t = table_factory(d)
    c = LevelComparer(
        "names",
        [
            dict(
                name="any_equal",
                # could do the same with ibis's Array.intersect,
                # but I want to check this works with a custom function
                condition=_array_any(
                    _.names_l.map(
                        lambda left: _array_any(
                            _.names_r.map(lambda right: left == right)
                        )
                    )
                ),
            ),
        ],
    )
    t = compare(t, c)
    assert (t.result == t.expected).all().execute()


def _array_any(x):
    return x.filter(lambda x: x).length() > 0
