"""
Blocking is the process of selecting which pairs of records should be further compared.

Because pairwise comparisons is an O(n^2) operation, it is infeasible to compare all
pairs of records. Therefore, we use blocking to reduce the number of pairs that need
to be compared, hopefully to a manageable level.
"""
from __future__ import annotations

from mismo.block._blocker import Blocking as Blocking
from mismo.block._blocker import CartesianBlocker as CartesianBlocker
from mismo.block._blocker import FunctionBlocker as FunctionBlocker
from mismo.block._blocker import PBlocker as PBlocker
from mismo.block._blocker import PBlocking as PBlocking
