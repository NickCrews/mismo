"""Tools for the Fellegi-Sunter model of comparison record pairs."""

from __future__ import annotations

from ._base import ComparisonWeights as ComparisonWeights
from ._base import LevelWeights as LevelWeights
from ._base import Weights as Weights
from ._train import train_comparison as train_comparison
from ._train import train_comparisons as train_comparisons
