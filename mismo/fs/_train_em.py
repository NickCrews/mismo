from __future__ import annotations

import logging
from typing import Iterable

from ibis import _
from ibis.expr import types as ir

from mismo.compare import LevelComparer

from . import _train
from ._weights import ComparerWeights, Weights

logger = logging.getLogger(__name__)


def train_using_em(
    comparers: Iterable[LevelComparer],
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int | None = None,
) -> Weights:
    """Train weights on unlabeled data using an expectation maximization algorithm.

    Parameters
    ----------
    comparers
        The comparers to train.
    left
        The left dataset.
    right
        The right dataset.
    max_pairs
        The maximum number of pairs to sample.
        If None, all pairs are used.

    Returns
    -------
    Weights
        The estimated weights for each comparer.
    """
    links = _train.sample_all_links(left, right, max_pairs=max_pairs)
    links = links.with_both()
    for c in comparers:
        links = c(links)
    links = links.select([c.name for c in comparers])
    links = links.cache()
    weights = _initial_weights(comparers, links)
    for i in range(5):
        logger.info("EM iteration {i}, starting weights: {weights}", i, weights)
        scored = weights.score_compared(links)
        is_match = _.odds >= 10
        matches = scored.filter(is_match)
        nonmatches = scored.filter(~is_match)
        weights = _weights_from_matches_nonmatches(comparers, matches, nonmatches)
    return weights


def _initial_weights(comparers: Iterable[LevelComparer], labels: ir.Table) -> Weights:
    return Weights(_initial_comparer_weights(c, labels[c.name]) for c in comparers)


def _initial_comparer_weights(
    comparer: LevelComparer, labels: ir.IntegerColumn
) -> ComparerWeights:
    n_levels = len(comparer.levels)
    ms = [1 / n_levels] * n_levels
    us = _train.level_proportions(comparer.levels, labels)
    return _train.make_weights(comparer, ms, us)


def _weights_from_matches_nonmatches(
    comparers: Iterable[LevelComparer], matches: ir.Table, nonmatches: ir.Table
) -> Weights:
    return Weights(
        [
            _comparer_weights_from_matches_nonmatches(
                comp, matches[comp.name], nonmatches[comp.name]
            )
            for comp in comparers
        ]
    )


def _comparer_weights_from_matches_nonmatches(
    comparer: LevelComparer,
    match_labels: ir.IntegerColumn | ir.StringColumn,
    nonmatch_labels: ir.IntegerColumn | ir.StringColumn,
) -> ComparerWeights:
    ms = _train.level_proportions(comparer.levels, match_labels)
    us = _train.level_proportions(comparer.levels, nonmatch_labels)
    return _train.make_weights(comparer, ms, us)
