from __future__ import annotations

from ibis.expr.types import Table

from mismo.block._blocker import PBlocker, PBlocking
from mismo.compare import PComparer
from mismo.partition import PPartitioner


class Deduper:
    def __init__(
        self,
        blocker: PBlocker,
        comparer: PComparer,
        partitioner: PPartitioner,
    ):
        self.blocker = blocker
        self.comparer = comparer
        self.partitioner = partitioner

    def block(self, data: Table) -> PBlocking:
        return self.blocker.block(data)

    def compare(self, blocking: PBlocking) -> Table:
        return self.comparer.compare(blocking)

    def partition(self, comparisons: Table) -> Table:
        return self.partitioner.partition(comparisons)

    def dedupe(self, data: Table) -> Table:
        blocking = self.block(data)
        comparisons = self.compare(blocking)
        partitions = self.partition(comparisons)
        return partitions
