"""Compare module.

This module contains functions and classes for comparing pairs of records.
"""

from __future__ import annotations

from ibis_enum import IbisEnum as IbisEnum

from mismo.compare._comparer import PComparer as PComparer
from mismo.compare._enum_comparer import EnumComparer as EnumComparer
from mismo.compare._plot import compared_dashboard as compared_dashboard
