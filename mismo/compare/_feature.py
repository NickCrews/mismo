"""An implementation of a Comparer that uses a feature-based approach.

This is the approach that dedupe and recordlinkage, and most other libraries use:
Generate a set of features between two records (eg string distance between two names,
cosine similarity between two lat/longs, etc), and then use a machine learning model to
classify the pairs as matches or non-matches.
"""
from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table

from mismo.block import PBlocking
from mismo.compare import Comparisons, PComparer, PComparisons


class PFeaturizer(Protocol):
    def features(self, blocking: PBlocking) -> Table:
        ...


class PScorer(Protocol):
    def score(self, features: Table) -> Table:
        ...


class FeatureComparer(PComparer):
    """A Comparer that generates pairwise features and then scores them"""

    def __init__(self, featurizer: PFeaturizer, scorer: PScorer):
        self.featurizer = featurizer
        self.scorer = scorer

    def compare(self, blocking: PBlocking) -> PComparisons:
        features = self.featurizer.features(blocking)
        scores = self.scorer.score(features)
        return Comparisons(blocking=blocking, compared=scores)
