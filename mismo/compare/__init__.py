"""Compare module.

This module contains functions and classes for comparing pairs of records.
"""

from __future__ import annotations

from . import fs as fs
from ._array import intersection_n as intersection_n
from ._array import jaccard as jaccard
from ._comparison import Comparison as Comparison
from ._comparison import ComparisonLevel as ComparisonLevel
from ._comparison import Comparisons as Comparisons
from ._geospatial import distance_km as distance_km
from ._levels import exact_level as exact_level
from ._plot import plot_compared as plot_compared
