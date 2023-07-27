from __future__ import annotations

import pytest

from mismo import examples
from mismo.compare._comparison import Comparison, ComparisonLevel
from mismo.compare.fs import _levels as levels_lib
from mismo.compare.fs._base import FSComparison
from mismo.compare.fs._train import train_comparison


# TODO: Maybe the sampling isn't deterministic, and that's why the weights are
# wrong now?
@pytest.mark.xfail(reason="Something broke the weights")
def test_comparison_training():
    """Test that training a Comparison works."""
    patents = examples.load_patents()
    left, right = patents, patents.view()
    almost_level = ComparisonLevel(
        name="almost",
        condition=lambda table: table["Name_l"][:3] == table["Name_r"][:3],  # type: ignore # noqa: E501
        description="First 3 letters match",
    )
    exact_level = levels_lib.exact("Name")
    levels = [exact_level, almost_level]
    comparison = Comparison(name="Name", levels=levels)
    fscomparison = FSComparison(comparison)
    trained = train_comparison(fscomparison, left, right, max_pairs=10_000, seed=42)
    assert trained is not None
    assert trained.name == "Name"
    assert len(trained.levels) == 2

    exact, almost = trained.levels
    assert almost.name == "almost"
    assert exact.name == "exact_Name"

    exact_weights, almost_weights = trained.weights
    assert exact_weights is not None
    assert exact_weights.m > 0.2
    assert exact_weights.m < 0.5
    assert exact_weights.u > 0
    assert exact_weights.u < 0.1

    assert almost_weights is not None
    assert almost_weights.m > 0.2
    assert almost_weights.m < 0.5
    assert almost_weights.u > 0
    assert almost_weights.u < 0.1
