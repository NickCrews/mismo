"""
Blocking is the process of selecting which pairs of records should be further compared.

Because pairwise comparisons is an O(n^2) operation, it is infeasible to compare all
pairs of records. Therefore, we use blocking to reduce the number of pairs that need
to be compared, hopefully to a manageable level.
"""
from __future__ import annotations

from mismo.block._array import block_on_arrays as block_on_arrays
from mismo.block._blocking import BlockedBlocking as BlockedBlocking
from mismo.block._blocking import Blocking as Blocking
from mismo.block._blocking import IdsBlocking as IdsBlocking
from mismo.block._blocking import block as block

# from mismo.block._base import blocking as blocking
from mismo.block._util import join as join

__all__ = [
    "block_on_arrays",
    "BlockedBlocking",
    "IdsBlocking",
    "Blocking",
    "join",
    "block",
]
