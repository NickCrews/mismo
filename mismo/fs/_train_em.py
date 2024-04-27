from __future__ import annotations

import logging
from typing import Iterable

from ibis import _
from ibis.expr import types as ir

from mismo.compare import LevelComparer, compare

from . import _train
from ._weights import ComparisonWeights, Weights

logger = logging.getLogger(__name__)


def train_using_em(
    comparers: Iterable[LevelComparer],
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> Weights:
    """Train weights on unlabeled data using an expectation maximization algorithm."""
    initial_blocking = _train.sample_all_pairs(
        left, right, max_pairs=max_pairs, seed=seed
    )
    compared = compare(initial_blocking, *comparers)
    compared = compared[[c.name for c in comparers]]
    compared = compared.cache()
    weights = _initial_weights(comparers, compared)
    for i in range(5):
        logger.info("EM iteration {i}, starting weights: {weights}", i, weights)
        scored = weights.score_compared(compared)
        is_match = _.odds >= 10
        matches = scored.filter(is_match)
        nonmatches = scored.filter(~is_match)
        weights = _weights_from_matches_nonmatches(comparers, matches, nonmatches)
    return weights


def _initial_weights(comparisons: Iterable[LevelComparer], labels: ir.Table) -> Weights:
    return Weights(_initial_comparison_weights(c, labels[c.name]) for c in comparisons)


def _initial_comparison_weights(
    comparison: LevelComparer, labels: ir.IntegerColumn
) -> ComparisonWeights:
    n_levels = len(comparison)
    ms = [1 / n_levels] * n_levels
    us = _train.level_proportions(comparison, labels)
    return _train.make_weights(comparison, ms, us)


def _weights_from_matches_nonmatches(
    comparisons: Iterable[LevelComparer], matches: ir.Table, nonmatches: ir.Table
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
    comparison: LevelComparer,
    match_labels: ir.IntegerColumn | ir.StringColumn,
    nonmatch_labels: ir.IntegerColumn | ir.StringColumn,
) -> ComparisonWeights:
    ms = _train.level_proportions(comparison, match_labels)
    us = _train.level_proportions(comparison, nonmatch_labels)
    return _train.make_weights(comparison, ms, us)
