from __future__ import annotations

from mismo import datasets
from mismo.compare import Comparison, ComparisonLevel, exact_level
from mismo.compare.fs import train_comparison


def test_comparison_training():
    """Test that finding the weights for a Comparison works."""
    patents = datasets.load_patents()
    left, right = patents, patents.view()
    almost_level = ComparisonLevel(
        name="almost",
        condition=lambda table: table["name_l"][:3] == table["name_r"][:3],  # type: ignore # noqa: E501
        description="First 3 letters match",
    )
    exact = exact_level("name")
    levels = [exact, almost_level]
    comparison = Comparison(name="name", levels=levels)
    weights = train_comparison(comparison, left, right, max_pairs=10_000, seed=42)
    assert weights.name == "name"
    assert len(weights.level_weights) == 2

    exact, almost = weights.level_weights

    assert exact is not None
    assert exact.name == "exact_name"
    assert exact.m > 0.03
    assert exact.m < 0.06
    assert exact.u > 0
    assert exact.u < 0.01

    assert almost is not None
    assert almost.name == "almost"
    assert almost.m > 0.2
    assert almost.m < 0.5
    assert almost.u > 0
    assert almost.u < 0.1
