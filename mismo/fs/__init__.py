"""Tools for the Fellegi-Sunter model of comparison record pairs."""

from __future__ import annotations

from ._plot import plot_weights as plot_weights
from ._train import train_comparison as train_comparison
from ._train import train_comparisons as train_comparisons
from ._train_em import train_using_em as train_using_em
from ._weights import ComparisonWeights as ComparisonWeights
from ._weights import LevelWeights as LevelWeights
from ._weights import Weights as Weights
