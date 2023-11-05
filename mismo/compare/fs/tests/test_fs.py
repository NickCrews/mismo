from __future__ import annotations

from ibis import _

from mismo import datasets
from mismo.compare import Comparison, ComparisonLevel
from mismo.compare.fs import train_comparison


def test_comparison_training():
    """Test that finding the weights for a Comparison works."""
    patents = datasets.load_patents()
    left, right = patents, patents.view()
    almost_level = ComparisonLevel(
        name="almost",
        condition=lambda table: table["name_l"][:3] == table["name_r"][:3],
        description="First 3 letters match",
    )
    ex_level = ComparisonLevel("exact", _.name_l == _.name_r)
    levels = [ex_level, almost_level]
    comparison = Comparison(name="name", levels=levels)
    weights = train_comparison(comparison, left, right, max_pairs=10_000, seed=42)
    assert weights.name == "name"
    assert len(weights) == 3  # 2 levels + 1 ELSE

    exact, almost, else_ = weights

    assert exact.name == "exact"
    assert exact.m > 0.03
    assert exact.m < 0.06
    assert exact.u > 0
    assert exact.u < 0.01

    assert almost.name == "almost"
    assert almost.m > 0.2
    assert almost.m < 0.5
    assert almost.u > 0
    assert almost.u < 0.1

    assert else_.name == "else"
    assert else_.m > 0.6
    assert else_.m < 0.7
    assert else_.u > 0.9
    assert else_.u < 1
