import pandas as pd
from vaex.dataframe import DataFrame

from mismo.block import PBlocker
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

    def block(self, data: DataFrame) -> DataFrame:
        return self.blocker.block(data, data)

    def score(self, data: DataFrame, links: DataFrame) -> DataFrame:
        return self.scorer.score(data, data, links)

    def partition(self, scores: DataFrame) -> DataFrame:
        return self.partitioner.partition(scores)

    def dedupe(self, data: DataFrame) -> pd.Series:
        links = self.block(data)
        scores = self.score(data, links)
        partitions = self.partition(scores)
        return partitions
