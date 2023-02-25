from ibis.expr.types import Table

from mismo.block._blocker import PBlocker
from mismo.partition import PPartitioner
from mismo.score import PScorer


class Deduper:
    def __init__(
        self,
        blocker: PBlocker,
        scorer: PScorer,
        partitioner: PPartitioner,
    ):
        self.blocker = blocker
        self.scorer = scorer
        self.partitioner = partitioner

    def block(self, data: Table) -> Table:
        return self.blocker.block(data)

    def score(self, blocking: Table) -> Table:
        return self.scorer.score(blocking)

    def partition(self, scores: Table) -> Table:
        return self.partitioner.partition(scores)

    def dedupe(self, data: Table) -> Table:
        blocking = self.block(data)
        scores = self.score(blocking)
        partitions = self.partition(scores)
        return partitions
