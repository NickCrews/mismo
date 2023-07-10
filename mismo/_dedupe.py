from __future__ import annotations

from ibis.expr.types import Table

from mismo._dataset import DedupeDatasetPair, PDatasetPair
from mismo.block import PBlocker, PBlocking
from mismo.cluster import PClusterer
from mismo.compare import PComparer, PComparisons


class Deduper:
    def __init__(
        self,
        blocker: PBlocker,
        comparer: PComparer,
        clusterer: PClusterer,
    ):
        self.blocker = blocker
        self.comparer = comparer
        self.clusterer = clusterer

    def block(self, dataset_pair: PDatasetPair) -> PBlocking:
        return self.blocker.block(dataset_pair)

    def compare(self, blocking: PBlocking) -> PComparisons:
        return self.comparer.compare(blocking)

    def cluster(self, comparisons: PComparisons) -> Table:
        return self.clusterer.cluster(comparisons)

    def dedupe(self, dataset: Table) -> Table:
        ddsp = DedupeDatasetPair(dataset)
        blocking = self.block(ddsp)
        comparisons = self.compare(blocking)
        clusters = self.cluster(comparisons)
        return clusters
