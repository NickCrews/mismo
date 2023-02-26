from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table


class PPartitioner(Protocol):
    """Takes a set if pairwise scores and partitions the records into groups.

    You can think of this as a graph algorithm, where the nodes are the records,
    and the edges are the pairwise scores. The algorithm determines the "clusters"
    of records within the graph and assigns a cluster ID to each record.
    """

    def partition(self, scores: Table) -> Table:
        ...
