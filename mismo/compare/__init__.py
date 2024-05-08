"""Compare module.

This module contains functions and classes for comparing pairs of records.
"""

from __future__ import annotations

from mismo.compare._array import jaccard as jaccard
from mismo.compare._compare import compare as compare
from mismo.compare._level_comparer import AgreementLevel as AgreementLevel
from mismo.compare._level_comparer import LevelComparer as LevelComparer
from mismo.compare._match_level import MatchLevel as MatchLevel
from mismo.compare._plot import compared_dashboard as compared_dashboard
