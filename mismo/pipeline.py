from typing import Protocol

import modin.pandas as pd

from mismo._typing import ClusterIds, Data, Features, Links, Scores
from mismo.block import PBlocker


class PFeaturizer(Protocol):
    def featurize(self, data1: Data, data2: Data, links: Links) -> Features:
        ...


class PScorer(Protocol):
    def score(self, features: Features) -> Scores:
        ...


class PClusterer(Protocol):
    def cluster(self, links: Links, scores: Scores) -> ClusterIds:
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

    def block(self, data1: Data, data2: Data) -> Links:
        return self.blocker.block(data1, data2)

    def featurize(self, data1: Data, data2: Data, links: Links) -> Features:
        return self.featurizer.featurize(data1, data2, links)

    def score(self, data: Features) -> Scores:
        return self.scorer.score(data)

    def cluster(self, links: Links, scores: Scores) -> ClusterIds:
        return self.clusterer.cluster(links, scores)

    def dedupe(self, data: Data) -> pd.Series:
        links = self.block(data, data)
        features = self.featurize(data, data, links)
        scores = self.score(features)
        clusters = self.cluster(links, scores)
        return clusters
