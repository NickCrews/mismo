"""Compare module.

This module contains functions and classes for comparing pairs of records.
"""

from __future__ import annotations

from ._array import jaccard as jaccard
from ._compare import compare as compare
from ._level_comparer import AgreementLevel as AgreementLevel
from ._level_comparer import LevelComparer as LevelComparer
from ._plot import compared_dashboard as compared_dashboard
