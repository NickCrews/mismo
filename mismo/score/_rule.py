"""An implementation of a Scorer that uses a rule-based approach.

This might be useful if you want to guarantee that if two records have the same
first name, they are a match, or if two records have different lat/longs, they
are not a match.
"""
from typing import Callable

from vaex.dataframe import DataFrame

from mismo.score import PScorer

Rule = Callable[[DataFrame, DataFrame, DataFrame], DataFrame]


class RuleScorer(PScorer):
    def __init__(self, rules: list[Rule]):
        self.rules = rules

    def score(self, datal: DataFrame, datar: DataFrame, links: DataFrame) -> DataFrame:
        scores = links.copy()
        scores["score"] = None
        for rule in self.rules:
            new_scores = rule(datal, datar, scores)
            to_update = new_scores.notna()
            scores["score", to_update] = new_scores[to_update]
        return scores
