from __future__ import annotations

from mismo import examples
from mismo._dataset import DedupeDatasetPair
from mismo.compare.fs import _levels as levels_lib
from mismo.compare.fs._base import Comparison, ComparisonLevel
from mismo.compare.fs._train import train_comparison


def test_comparison_training():
    """Test that training a Comparison works."""
    patents = examples.load_patents()
    patents_dataset_pair = DedupeDatasetPair(patents)
    almost_level = ComparisonLevel(
        name="almost",
        predicate=lambda table: table["Name_l"][:3] == table["Name_r"][:3],  # type: ignore # noqa: E501
        description="First 3 letters match",
    )
    exact_level = levels_lib.exact("Name")
    levels = [exact_level, almost_level]
    comparison = Comparison(name="Name", levels=levels)
    trained = train_comparison(
        comparison, patents_dataset_pair, max_pairs=10_000, seed=42
    )
    assert trained is not None
    assert trained.name == "Name"
    assert len(trained.levels) == 2

    exact, almost = trained.levels
    assert exact.name == "exact_Name"
    assert exact.weights is not None
    assert exact.weights.m > 0.2
    assert exact.weights.m < 0.5
    assert exact.weights.u > 0
    assert exact.weights.u < 0.1

    assert almost.name == "almost"
    assert almost.weights is not None
    assert almost.weights.m > 0.2
    assert almost.weights.m < 0.5
    assert almost.weights.u > 0
    assert almost.weights.u < 0.1
