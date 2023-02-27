from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table

from mismo.compare import PComparisons


class PPartitioner(Protocol):
    """Takes a set if pairwise comparisons and partitions the records into groups.

    You can think of this as a graph algorithm, where the nodes are the records,
    and the edges are the comparisons. The algorithm determines the "clusters"
    of records within the graph and assigns a cluster ID to each record.
    """

    def partition(self, comparisons: PComparisons) -> Table:
        ...
