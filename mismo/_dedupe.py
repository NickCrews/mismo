from __future__ import annotations

from ibis.expr.types import Table

from mismo._dataset import DedupeDatasetPair, PDatasetPair
from mismo.block import Blocker, Blocking
from mismo.cluster import PClusterer
from mismo.compare import PComparer, PComparisons


class Deduper:
    def __init__(
        self,
        blocker: Blocker,
        comparer: PComparer,
        clusterer: PClusterer,
    ):
        self.blocker = blocker
        self.comparer = comparer
        self.clusterer = clusterer

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
        return self.block(dataset_pair, self.blocker)

    def compare(self, blocking: Blocking) -> PComparisons:
        return self.comparer.compare(blocking)

    def cluster(self, comparisons: PComparisons) -> Table:
        return self.clusterer.cluster(comparisons)

    def dedupe(self, dataset: Table) -> Table:
        ddsp = DedupeDatasetPair(dataset)
        blocking = self.block(ddsp)
        comparisons = self.compare(blocking)
        clusters = self.cluster(comparisons)
        return clusters
