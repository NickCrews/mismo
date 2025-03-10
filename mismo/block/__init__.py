"""
Blocking is the process of selecting which pairs of records should be further compared.

Because pairwise comparisons is an O(n^2) operation, it is infeasible to compare all
pairs of records. Therefore, we use blocking to reduce the number of pairs that need
to be compared, hopefully to a manageable level.
"""

from __future__ import annotations

from mismo.block._blocker import ConditionBlocker as ConditionBlocker
from mismo.block._blocker import CrossBlocker as CrossBlocker
from mismo.block._blocker import EmptyBlocker as EmptyBlocker
from mismo.block._blocker import PBlocker as PBlocker
from mismo.block._core import join as join
from mismo.block._counts_table import CountsTable as CountsTable
from mismo.block._ensemble import UnionBlocker as UnionBlocker
from mismo.block._key_blocker import KeyBlocker as KeyBlocker
from mismo.block._key_linkage_combined import UnionKeyLinkage as UnionKeyLinkage
from mismo.block._key_linkage_combined import register_combiners as _register_combiners
from mismo.block._metrics import n_naive_comparisons as n_naive_comparisons
from mismo.block._sql_analyze import JOIN_ALGORITHMS as JOIN_ALGORITHMS
from mismo.block._sql_analyze import SLOW_JOIN_ALGORITHMS as SLOW_JOIN_ALGORITHMS
from mismo.block._sql_analyze import SlowJoinError as SlowJoinError
from mismo.block._sql_analyze import SlowJoinWarning as SlowJoinWarning
from mismo.block._sql_analyze import check_join_algorithm as check_join_algorithm
from mismo.block._sql_analyze import get_join_algorithm as get_join_algorithm
from mismo.block._upset_block import upset_chart as upset_chart
from mismo.block._util import sample_all_pairs as sample_all_pairs

_register_combiners()
