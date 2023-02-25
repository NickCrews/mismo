"""An implementation of a Scorer that uses a feature-based approach.

This is the approach that dedupe and recordlinkage, and most other libraries use:
Generate a set of features between two records (eg string distance between two names,
cosine similarity between two lat/longs, etc), and then use a machine learning model to
classify the pairs as matches or non-matches.
"""

from ibis.expr.types import Table

from mismo._typing import Protocol
from mismo.score import PScorer


class PFeaturizer(Protocol):
    def features(self, blocked: Table) -> Table:
        ...


class PClassifier(Protocol):
    def predict(self, features: Table) -> Table:
        ...


class FeatureScorer(PScorer):
    """A Scorer that generates pairwise features and uses a classifier to score them."""

    def __init__(self, featurizer: PFeaturizer, classifier: PClassifier):
        self.featurizer = featurizer
        self.classifier = classifier

    def score(self, blocked: Table) -> Table:
        features = self.featurizer.features(blocked)
        scores = self.classifier.predict(features)
        return scores
