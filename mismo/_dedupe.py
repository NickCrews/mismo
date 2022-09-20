import pandas as pd
from vaex.dataframe import DataFrame

from mismo.block._blocker import PBlocker, PBlocking
from mismo.partition import PPartitioner
from mismo.score import PScorer, PScoring


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

    def block(self, data: DataFrame) -> PBlocking:
        return self.blocker.block(data)

    def score(self, data: DataFrame, blocking: PBlocking) -> PScoring:
        return self.scorer.score(data, data, blocking)

    def partition(self, scores: PScoring) -> DataFrame:
        return self.partitioner.partition(scores)

    def dedupe(self, data: DataFrame) -> pd.Series:
        blocking = self.block(data)
        scores = self.score(data, blocking)
        partitions = self.partition(scores)
        return partitions
