from __future__ import annotations

import logging

from ibis import _
from ibis.expr.types import IntegerColumn, StringColumn, Table

from mismo.compare._comparison import Comparison, Comparisons

from . import _train
from ._weights import ComparisonWeights, Weights

logger = logging.getLogger(__name__)


def train_using_em(
    comparisons: Comparisons,
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> Weights:
    """Train weights on unlabeled data using an expectation maximization algorithm."""
    initial_blocking = _train.all_possible_pairs(
        left, right, max_pairs=max_pairs, seed=seed
    )
    compared = comparisons.label_pairs(initial_blocking)
    compared = compared[[c.name for c in comparisons]]
    compared = compared.cache()
    weights = _initial_weights(comparisons, compared)
    for i in range(5):
        logger.info("EM iteration {i}, starting weights: {weights}", i, weights)
        scored = weights.score(compared)
        is_match = _.odds >= 10
        matches = scored.filter(is_match)
        nonmatches = scored.filter(~is_match)
        weights = _weights_from_matches_nonmatches(comparisons, matches, nonmatches)
    return weights


def _initial_weights(comparisons: Comparisons, labels: Table) -> Weights:
    return Weights(_initial_comparison_weights(c, labels[c.name]) for c in comparisons)


def _initial_comparison_weights(
    comparison: Comparison, labels: IntegerColumn
) -> ComparisonWeights:
    n_levels = len(comparison)
    ms = [1 / n_levels] * n_levels
    us = _train.level_proportions(comparison, labels)
    return _train.make_weights(comparison, ms, us)


def _weights_from_matches_nonmatches(
    comparisons: Comparisons, matches: Table, nonmatches: Table
) -> Weights:
    return Weights(
        [
            _comparison_weights_from_matches_nonmatches(
                comp, matches[comp.name], nonmatches[comp.name]
            )
            for comp in comparisons
        ]
    )


def _comparison_weights_from_matches_nonmatches(
    comparison: Comparison,
    match_labels: IntegerColumn | StringColumn,
    nonmatch_labels: IntegerColumn | StringColumn,
) -> ComparisonWeights:
    ms = _train.level_proportions(comparison, match_labels)
    us = _train.level_proportions(comparison, nonmatch_labels)
    return _train.make_weights(comparison, ms, us)
