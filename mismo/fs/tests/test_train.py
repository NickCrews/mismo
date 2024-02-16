from __future__ import annotations

from ibis import _
import pytest

from mismo import datasets, fs
from mismo.compare import Comparison, ComparisonLevel, Comparisons
from mismo.lib.geo import distance_km


@pytest.fixture
def name_comparison():
    return Comparison(
        name="name",
        levels=[
            ComparisonLevel("exact", _.name_l == _.name_r),
            ComparisonLevel("close", _.name_l[:3] == _.name_r[:3]),
        ],
    )


@pytest.fixture
def location_comparison():
    return Comparison(
        name="location",
        levels=[
            ComparisonLevel(
                "exact",
                (_.latitude_l == _.latitude_r) & (_.longitude_l == _.longitude_r),
            ),
            ComparisonLevel(
                "coords <= 10km",
                distance_km(
                    lat1=_.latitude_l,
                    lon1=_.longitude_l,
                    lat2=_.latitude_r,
                    lon2=_.longitude_r,
                )
                <= 10,
            ),
            ComparisonLevel(
                "coords <= 100km",
                distance_km(
                    lat1=_.latitude_l,
                    lon1=_.longitude_l,
                    lat2=_.latitude_r,
                    lon2=_.longitude_r,
                )
                <= 100,
            ),
            ComparisonLevel(
                "both coord missing", _.latitude_l.isnull() & _.latitude_r.isnull()
            ),
            ComparisonLevel(
                "one coord missing", _.latitude_l.isnull() | _.latitude_r.isnull()
            ),
        ],
    )


def test_train_comparison_from_labels(backend, name_comparison):
    """Test that finding the weights for a Comparison works."""
    patents = datasets.load_patents(backend)
    left, right = patents, patents.view()
    weights = fs.train_comparison_using_labels(
        name_comparison, left, right, max_pairs=1_000, seed=42
    )
    assert weights.name == "name"
    assert len(weights) == 3  # 2 levels + 1 ELSE

    exact, close, else_ = weights

    assert exact.name == "exact"
    assert exact.m == pytest.approx(0.0507, rel=0.2)
    assert exact.u == pytest.approx(0.00207, rel=0.4)

    assert close.name == "close"
    assert close.m == pytest.approx(0.3522, rel=0.3)
    assert close.u == pytest.approx(0.03623, rel=0.4)

    assert else_.name == "else"
    assert else_.m == pytest.approx(0.5971, rel=0.2)
    assert else_.u == pytest.approx(0.9617, rel=0.2)


# TODO: Actually check that these weights are correct
# At this point this just checks that there are no errors
def test_train_comparions_using_em(backend, name_comparison, location_comparison):
    patents = datasets.load_patents(backend)
    left, right = patents, patents.view()
    weights = fs.train_comparisons_using_em(
        Comparisons([name_comparison, location_comparison]),
        left,
        right,
        max_pairs=100_000,
        seed=41,
    )
    assert len(weights) == 2
    exact, close, else_ = weights["name"]

    assert exact.name == "exact"
    assert exact.m == pytest.approx(0.999, rel=0.1)
    assert exact.u == pytest.approx(0.006, rel=0.1)

    assert close.name == "close"
    assert close.m == pytest.approx(0.0027, rel=0.1)
    assert close.u == pytest.approx(0.067, rel=0.1)

    assert else_.name == "else"
    assert else_.m == pytest.approx(0.0027, rel=0.1)
    assert else_.u == pytest.approx(0.93, rel=0.1)
