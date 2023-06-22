from __future__ import annotations

from ibis.expr.types import Table

from mismo._dataset import DedupeDatasetPair, PDatasetPair
from mismo.block._blocker import PBlocker, PBlocking
from mismo.compare._base import PComparer, PComparisons
from mismo.partition._base import PPartitioner


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

    def block(self, dataset_pair: PDatasetPair) -> PBlocking:
        return self.blocker.block(dataset_pair)

    def compare(self, blocking: PBlocking) -> PComparisons:
        return self.comparer.compare(blocking)

    def partition(self, comparisons: PComparisons) -> Table:
        return self.partitioner.partition(comparisons)

    def dedupe(self, dataset: Table) -> Table:
        ddsp = DedupeDatasetPair(dataset)
        blocking = self.block(ddsp)
        comparisons = self.compare(blocking)
        partitions = self.partition(comparisons)
        return partitions
