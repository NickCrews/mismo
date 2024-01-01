from __future__ import annotations

from ibis import _

from mismo import datasets
from mismo.compare import Comparison, ComparisonLevel
from mismo.fs import train_comparison


def test_train_comparison_from_labels(backend):
    """Test that finding the weights for a Comparison works."""
    patents = datasets.load_patents(backend)
    left, right = patents, patents.view()
    close_level = ComparisonLevel(name="close", condition=_.name_l[:3] == _.name_r[:3])
    ex_level = ComparisonLevel("exact", _.name_l == _.name_r)
    comparison = Comparison(name="name", levels=[ex_level, close_level])
    weights = train_comparison(comparison, left, right, max_pairs=1_000, seed=42)
    assert weights.name == "name"
    assert len(weights) == 3  # 2 levels + 1 ELSE

    exact, close, else_ = weights

    assert exact.name == "exact"
    assert exact.m > 0.03
    assert exact.m < 0.06
    assert exact.u > 0
    assert exact.u < 0.02

    assert close.name == "close"
    assert close.m > 0.2
    assert close.m < 0.5
    assert close.u > 0
    assert close.u < 0.1

    assert else_.name == "else"
    assert else_.m > 0.6
    assert else_.m < 0.7
    assert else_.u > 0.9
    assert else_.u < 1
