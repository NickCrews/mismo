"""
Blocking is the process of selecting which pairs of records should be further compared.

Because pairwise comparisons is an O(n^2) operation, it is infeasible to compare all
pairs of records. Therefore, we use blocking to reduce the number of pairs that need
to be compared, hopefully to a manageable level.
"""

from __future__ import annotations

from mismo.block._analyze import key_counts as key_counts
from mismo.block._block import block_many as block_many
from mismo.block._block import block_one as block_one
from mismo.block._block import join as join
from mismo.block._blocking_rule import BlockingRule as BlockingRule
from mismo.block._sql_analyze import JOIN_ALGORITHMS as JOIN_ALGORITHMS
from mismo.block._sql_analyze import SLOW_JOIN_ALGORITHMS as SLOW_JOIN_ALGORITHMS
from mismo.block._sql_analyze import SlowJoinError as SlowJoinError
from mismo.block._sql_analyze import SlowJoinWarning as SlowJoinWarning
from mismo.block._sql_analyze import check_join_algorithm as check_join_algorithm
from mismo.block._sql_analyze import get_join_algorithm as get_join_algorithm
from mismo.block._upset_block import upset_chart as upset_chart
from mismo.block._util import sample_all_pairs as sample_all_pairs
