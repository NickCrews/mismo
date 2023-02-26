"""
Blocking is the process of selecting which pairs of records should be further compared.

Because pairwise comparisons is an O(n^2) operation, it is infeasible to compare all
pairs of records. Therefore, we use blocking to reduce the number of pairs that need
to be compared, hopefully to a manageable level.
"""

from mismo.block._blocker import Blocking as Blocking  # noqa: F401
from mismo.block._blocker import PBlocker as PBlocker  # noqa: F401
from mismo.block._blocker import PBlocking as PBlocking  # noqa: F401
