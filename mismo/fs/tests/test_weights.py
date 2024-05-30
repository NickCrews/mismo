from __future__ import annotations

import ibis
import numpy as np
import pytest

from mismo.fs import ComparerWeights, LevelWeights, Weights


def test_level_weights_basic():
    lw = LevelWeights(name="close", m=0.5, u=0.05)
    assert lw.name == "close"
    assert lw.m == 0.5
    assert lw.u == 0.05
    assert lw.odds == 10
    assert lw.log_odds == 1
    assert lw == LevelWeights(name="close", m=0.5, u=0.05)
    assert lw != LevelWeights(name="oops", m=0.5, u=0.05)
    assert lw != 999
    assert repr(lw) == "LevelWeights(name=close, m=0.5, u=0.05)"

    # Can't set attributes
    with pytest.raises(AttributeError):
        lw.name = "a"
    with pytest.raises(AttributeError):
        lw.m = 0
    with pytest.raises(AttributeError):
        lw.u = 0
    with pytest.raises(AttributeError):
        lw.odds = 0


def test_level_weights_zero():
    lw = LevelWeights(name="close", m=0, u=0.6)
    assert lw.odds == 0
    assert lw.log_odds == float("-inf")


def test_level_weights_inf():
    lw = LevelWeights(name="close", m=0.5, u=0)
    assert lw.odds == float("inf")
    assert lw.log_odds == float("inf")


def test_level_weights_eq():
    lw1 = LevelWeights(name="close", m=0.5, u=0.05)
    lw2 = LevelWeights(name="close", m=0.5, u=0.05)
    lw3 = LevelWeights(name="close", m=0.5, u=0.05001)
    lw4 = LevelWeights(name="close", m=0.5, u=0.05000000001)
    assert lw1 == lw2
    assert lw1 != 999
    assert lw1 != lw3
    assert lw1 == lw4


def test_comparer_weights_basic():
    close_lw = LevelWeights(name="close", m=0.3, u=0.1)
    exact_lw = LevelWeights(name="exact", m=0.6, u=0.2)
    else_lw = LevelWeights(name="else", m=0.1, u=0.7)
    cw = ComparerWeights(
        name="address",
        level_weights=[close_lw, exact_lw, else_lw],
    )
    assert cw.name == "address"
    assert "close" in cw
    assert "exact" in cw
    assert "else" in cw
    assert "oops" not in cw
    assert 0 in cw
    assert 1 in cw
    assert 2 in cw
    assert 3 not in cw
    assert -1 in cw
    assert -2 in cw
    assert -3 in cw
    assert -4 not in cw

    assert len(cw) == 3
    assert cw["close"] == close_lw
    assert cw["exact"] == exact_lw
    assert cw["else"] == else_lw
    assert cw[0] == close_lw
    assert cw[1] == exact_lw
    assert cw[2] == else_lw
    assert cw[-1] == else_lw
    assert cw[-2] == exact_lw
    assert cw[-3] == close_lw
    with pytest.raises(KeyError):
        cw["oops"]
    with pytest.raises(KeyError):
        cw[3]
    with pytest.raises(KeyError):
        cw[-4]
    with pytest.raises(KeyError):
        cw[None]
    assert cw[:2] == (close_lw, exact_lw)
    assert cw[1:] == (exact_lw, else_lw)

    assert tuple(cw) == (close_lw, exact_lw, else_lw)
    assert list(cw) == [close_lw, exact_lw, else_lw]

    assert "address" in repr(cw)
    assert "close" in repr(cw)
    assert "exact" in repr(cw)
    assert "else" in repr(cw)


def test_comparer_weights_eq():
    close_lw = LevelWeights(name="close", m=0.3, u=0.1)
    exact_lw = LevelWeights(name="exact", m=0.6, u=0.2)
    cw1 = ComparerWeights(name="address", level_weights=[close_lw, exact_lw])
    cw2 = ComparerWeights(name="address", level_weights=[close_lw, exact_lw])
    cw3 = ComparerWeights(name="other", level_weights=[close_lw, exact_lw])
    cw4 = ComparerWeights(name="address", level_weights=[exact_lw, close_lw])
    assert cw1 == cw2
    assert cw1 != cw3
    assert cw1 != cw4
    assert cw1 != 999


def test_comparer_weights_odds():
    close_lw = LevelWeights(name="close", m=0.1, u=0.01)
    exact_lw = LevelWeights(name="exact", m=0.6, u=0.3)
    cw = ComparerWeights(name="address", level_weights=[close_lw, exact_lw])
    assert cw.odds("close") == 10
    assert cw.odds(0) == 10
    assert cw.odds("exact") == 2
    assert cw.odds(1) == 2

    with pytest.raises(KeyError):
        cw.odds("oops")
    with pytest.raises(KeyError):
        cw.odds(3)

    # When given an ibis expression, an ibis expression is returned
    assert cw.odds(ibis.literal("close")).execute() == 10
    assert cw.odds(ibis.literal(0)).execute() == 10
    # we can't have guardrails for giving bad levels :(
    assert np.isnan(cw.odds(ibis.literal("oops")).execute())
    assert np.isnan(cw.odds(ibis.literal(5)).execute())


def test_weights_serde(tmp_path):
    weights = Weights(
        [
            ComparerWeights(
                name="name",
                level_weights=[
                    LevelWeights(name="close", m=0.5, u=0.1),
                    LevelWeights(name="exact", m=0.6, u=0.9),
                ],
            ),
            ComparerWeights(
                name="address",
                level_weights=[
                    LevelWeights(name="within 10 km", m=0.5, u=0.1),
                    LevelWeights(name="street match", m=0.6, u=0.9),
                ],
            ),
        ]
    )
    d = weights.to_json(tmp_path / "weights.json")
    assert isinstance(d, dict)
    weights2 = Weights.from_json(tmp_path / "weights.json")
    weights3 = Weights.from_json(d)
    assert weights == weights2
    assert weights == weights3
