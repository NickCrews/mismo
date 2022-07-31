import pandas as pd
from vaex.dataframe import DataFrame

from mismo._typing import Protocol
from mismo.block import PBlocker


class PFeaturizer(Protocol):
    def featurize(
        self, data1: DataFrame, data2: DataFrame, links: DataFrame
    ) -> DataFrame:
        ...


class PScorer(Protocol):
    def score(self, features: DataFrame) -> DataFrame:
        ...


class PClusterer(Protocol):
    def cluster(self, links: DataFrame, scores: DataFrame) -> DataFrame:
        ...


class Pipeline:
    def __init__(
        self,
        blocker: PBlocker,
        featurizer: PFeaturizer,
        scorer: PScorer,
        clusterer: PClusterer,
    ):
        self.blocker = blocker
        self.featurizer = featurizer
        self.scorer = scorer
        self.clusterer = clusterer

    def block(self, data1: DataFrame, data2: DataFrame) -> DataFrame:
        return self.blocker.block(data1, data2)

    def featurize(
        self, data1: DataFrame, data2: DataFrame, links: DataFrame
    ) -> DataFrame:
        return self.featurizer.featurize(data1, data2, links)

    def score(self, data: DataFrame) -> DataFrame:
        return self.scorer.score(data)

    def cluster(self, links: DataFrame, scores: DataFrame) -> DataFrame:
        return self.clusterer.cluster(links, scores)

    def dedupe(self, data: DataFrame) -> pd.Series:
        links = self.block(data, data)
        features = self.featurize(data, data, links)
        scores = self.score(features)
        clusters = self.cluster(links, scores)
        return clusters
