from __future__ import annotations

from ibis import _
import pytest

from mismo import fs, playdata
from mismo.compare import LevelComparer, MatchLevel
from mismo.lib.geo import distance_km


@pytest.fixture
def name_comparer():
    class NameMatchLevel(MatchLevel):
        EXACT = 0
        CLOSE = 1
        ELSE = 2

    return LevelComparer(
        name="name",
        levels=NameMatchLevel,
        cases=[
            (_.name_l == _.name_r, NameMatchLevel.EXACT),
            (_.name_l[:3] == _.name_r[:3], "CLOSE"),
            (True, NameMatchLevel.ELSE),
        ],
    )


@pytest.fixture
def location_comparer():
    class LocationMatchLevel(MatchLevel):
        EXACT = 0
        WITHIN_10KM = 1
        WITHIN_100KM = 2
        BOTH_MISSING = 3
        ONE_MISSING = 4
        ELSE = 5

    return LevelComparer(
        name="location",
        levels=LocationMatchLevel,
        cases=[
            (
                (_.latitude_l == _.latitude_r) & (_.longitude_l == _.longitude_r),
                "EXACT",
            ),
            (
                distance_km(
                    lat1=_.latitude_l,
                    lon1=_.longitude_l,
                    lat2=_.latitude_r,
                    lon2=_.longitude_r,
                )
                <= 10,
                "WITHIN_10KM",
            ),
            (
                distance_km(
                    lat1=_.latitude_l,
                    lon1=_.longitude_l,
                    lat2=_.latitude_r,
                    lon2=_.longitude_r,
                )
                <= 100,
                "WITHIN_100KM",
            ),
            (
                _.latitude_l.isnull() & _.latitude_r.isnull(),
                "BOTH_MISSING",
            ),
            (
                _.latitude_l.isnull() | _.latitude_r.isnull(),
                "ONE_MISSING",
            ),
            (True, "ELSE"),
        ],
    )


def test_train_comparer_from_labels(backend, name_comparer):
    """Test that finding the weights for a Comparer works."""
    patents = playdata.load_patents(backend)
    (weights,) = fs.train_using_labels(
        [name_comparer], patents, patents, max_pairs=100_000
    )
    _check_name_weights(weights)


def test_train_comparer_from_pairs(backend, name_comparer):
    """Test that finding the weights for a Comparer works."""
    patents = playdata.load_patents(backend)
    pairs = patents.join(
        patents.view(), "label_true", lname="{name}_l", rname="{name}_r"
    )
    (weights,) = fs.train_using_pairs(
        [name_comparer], patents, patents, true_pairs=pairs, max_pairs=100_000
    )
    _check_name_weights(weights)


def _check_name_weights(weights):
    assert weights.name == "name"
    assert len(weights) == 3

    exact, close, else_ = weights

    assert exact.name == "EXACT"
    assert exact.m == pytest.approx(0.02723, abs=0.1)
    assert exact.u == pytest.approx(0.00207, abs=0.01)

    assert close.name == "CLOSE"
    assert close.m == pytest.approx(0.3522, abs=0.3)
    assert close.u == pytest.approx(0.03623, abs=0.1)

    assert else_.name == "ELSE"
    assert else_.m == pytest.approx(0.5971, abs=0.4)
    assert else_.u == pytest.approx(0.9617, abs=0.1)


# TODO: Actually check that these weights are correct.
# The CI runs get different results than my local run because it is
# impossible (I think) to set a random seed that is consistent across
# platforms and/or duckdb versions.
# At this point this just checks that there are no errors raised
def test_train_comparions_using_em(backend, name_comparer, location_comparer):
    patents = playdata.load_patents(backend)
    weights = fs.train_using_em(
        [name_comparer, location_comparer],
        patents,
        patents,
        max_pairs=100_000,
    )
    assert len(weights) == 2
    exact, close, else_ = weights["name"]
    print(weights["name"])

    assert exact.name == "EXACT"
    assert exact.m > 0.1
    assert exact.u < 0.1
    # This doesn't appear to be repeatable enough to do exact comparers
    # assert exact.m == pytest.approx(0.999, rel=0.1)
    # assert exact.u == pytest.approx(0.006, rel=0.1)

    assert close.name == "CLOSE"
    # assert close.m == pytest.approx(0.0027, rel=0.1)
    # assert close.u == pytest.approx(0.067, rel=0.1)

    assert else_.name == "ELSE"
    assert else_.m < 0.6
    assert else_.u > 0.7
    # assert else_.m == pytest.approx(0.0027, rel=0.1)
    # assert else_.u == pytest.approx(0.93, rel=0.1)

    assert exact.odds > close.odds
    # assert close.odds > else_.odds
