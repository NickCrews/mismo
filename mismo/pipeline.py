import pandas as pd
from vaex.dataframe import DataFrame

from mismo._typing import Protocol
from mismo.block import PBlocker
from mismo.compare import PComparer


class PScorer(Protocol):
    def score(self, features: DataFrame) -> DataFrame:
        ...


class PClusterer(Protocol):
    def cluster(self, scores: DataFrame) -> DataFrame:
        ...


class Pipeline:
    def __init__(
        self,
        blocker: PBlocker,
        comparer: PComparer,
        scorer: PScorer,
        clusterer: PClusterer,
    ):
        self.blocker = blocker
        self.comparer = comparer
        self.scorer = scorer
        self.clusterer = clusterer

    def block(self, datal: DataFrame, datar: DataFrame) -> DataFrame:
        return self.blocker.block(datal, datar)

    def compare(
        self, datal: DataFrame, datar: DataFrame, links: DataFrame
    ) -> DataFrame:
        return self.comparer.compare(datal, datar, links)

    def score(self, comparisons: DataFrame) -> DataFrame:
        return self.scorer.score(comparisons)

    def cluster(self, scores: DataFrame) -> DataFrame:
        return self.clusterer.cluster(scores)

    def dedupe(self, data: DataFrame) -> pd.Series:
        links = self.block(data, data)
        comparisons = self.compare(data, data, links)
        similarities = self.score(comparisons)
        clusters = self.cluster(similarities)
        return clusters
