"""A Comparer that uses the Fellegi-Sunter model to score pairs of records."""

from __future__ import annotations

from ._base import FellegiSunterComparer as FellegiSunterComparer
from ._base import FSComparison as FSComparison
from ._train import train_comparison as train_comparison
