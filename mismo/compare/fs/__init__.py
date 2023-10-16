"""Tools for the Fellegi-Sunter model of comparison record pairs."""

from __future__ import annotations

from ._train import train_comparison as train_comparison
from ._train import train_comparisons as train_comparisons
from ._weights import ComparisonWeights as ComparisonWeights
from ._weights import LevelWeights as LevelWeights
from ._weights import Weights as Weights
