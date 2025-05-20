from __future__ import annotations

from typing import Iterable, Type

from ibis import _
from ibis.expr import types as ir

from mismo._util import sample_table
from mismo.compare import LevelComparer, MatchLevel
from mismo.linkage import sample_all_links
from mismo.linker import JoinLinker

from ._weights import ComparerWeights, LevelWeights, Weights


def level_proportions(
    levels: Type[MatchLevel], labels: ir.IntegerColumn | ir.StringColumn
) -> list[float]:
    """
    Return the proportion of labels that fall into each [MatchLevel](mismo.compare.MatchLevel).
    """  # noqa: E501
    counts = (
        levels(labels)
        .as_integer()
        .name("level")
        .as_table()
        .group_by("level")
        .agg(n=_.count())
    )
    counts_dict: dict[int, int] = counts.execute().set_index("level")["n"].to_dict()
    # If we didn't see a level, that won't be present in the value_counts table.
    # Add it in, with a count of 1 to regularaize it.
    # If a level shows shows up 0 times among nonmatches, this would lead to an odds
    # of M/0 = infinity.
    # If it shows up 0 times among matches, this would lead to an odds of 0/M = 0.
    # To avoid this, for any levels that we didn't see, we pretend we saw them once.
    int_levels = [levels(lev).as_integer() for lev in levels]
    for lev in int_levels:
        counts_dict.setdefault(lev, 1)
    n_total = sum(counts_dict.values())
    return [counts_dict[lev] / n_total for lev in int_levels]


def _train_us_using_sampling(
    comparer: LevelComparer,
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int = 1_000_000_000,
) -> list[float]:
    """Estimate the u weight using random sampling.

    This is from splink's `estimate_u_using_random_sampling()`

    The u parameters represent the proportion of record pairs
    that fall into each MatchLevel amongst truly non-matching records.

    This procedure takes a sample of `left` and `right` and generates the cartesian
    product of record pairs.
    The validity of the u values rests on the assumption that nearly all of the
    resultant pairs are non-matches. For large datasets, this is typically true.

    Parameters
    ----------
    max_pairs
        The maximum number of pairwise record pairs to sample.
        Larger will give more accurate estimates but lead to longer runtimes.
        In our experience at least 1e9 (one billion)
        gives best results but can take a long time to compute.
        1e7 (ten million) is often adequate whilst testing different model
        specifications, before the final model is estimated.
    """
    sample = sample_all_links(left, right, max_pairs=max_pairs)
    sample = sample.with_both()
    labels = comparer(sample)[comparer.name]
    return level_proportions(comparer.levels, labels)


def _train_ms_from_pairs(
    comparer: LevelComparer,
    true_pairs: ir.Table,
    *,
    max_pairs: int = 1_000_000_000,
    seed: int | None = None,
) -> list[float]:
    """Estimate the m weights using the provided matching pairs.

    The m parameter represent the proportion of record pairs
    that fall into each MatchLevel amongst truly matching pairs.

    This function expects a table with columns `record_id_l` and `record_id_r`.
    Each row represents a pair of matching records. Non-matching pairs should
    not be included in this table.

    Parameters
    ----------
    comparer
        The comparer to train.
    true_pairs:
        Record pairs that are true matches.
    max_pairs
        The maximum number of pairs to sample.
    seed
        The random seed to use for sampling.

    Returns
    -------
    list[float]
        The estimated m weights.
    """

    n_pairs = min(true_pairs.count().execute(), max_pairs)
    sample = sample_table(true_pairs, n_pairs, seed=seed)
    labels = comparer(sample)[comparer.name]
    return level_proportions(comparer.levels, labels)


def _train_ms_from_labels(
    comparer: LevelComparer,
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int = 1_000_000_000,
    seed: int | None = None,
) -> list[float]:
    """Estimate the m weights using labeled records.

    The m parameter represent the proportion of record pairs
    that fall into each MatchLevel amongst truly matching pairs.

    This function expects a table of records with a column `label_true`.
    The `label_true` column is used to generate true-match record pairs.

    For example, if the entity being matched is persons, and your
    input dataset(s) contain social security number, this could be
    used to estimate the m values for the model.

    Note that this column does not need to be fully populated.
    A common case is where a unique identifier such as social
    security number is only partially populated.
    When NULL values are encountered in the ground truth column,
    that record is simply ignored.

    Parameters
    ----------
    comparer
        The comparer to train.
    left
        The left dataset. Must contain a column "label_true".
    right
        The right dataset. Must contain a column "label_true".
    max_pairs
        The maximum number of pairs to sample.
    seed
        The random seed to use for sampling.

    Returns
    -------
    list[float]
        The estimated m weights.
    """
    pairs = _true_pairs_from_labels(left, right)
    return _train_ms_from_pairs(comparer, pairs, max_pairs=max_pairs, seed=seed)


def _true_pairs_from_labels(left: ir.Table, right: ir.Table) -> ir.Table:
    if "label_true" not in left.columns:
        raise ValueError(
            f"Left dataset must have a label_true column. Found: {left.columns}"
        )
    if "label_true" not in right.columns:
        raise ValueError(
            f"Right dataset must have a label_true column. Found: {right.columns}"
        )
    return JoinLinker("label_true")(left, right).links


def train_using_pairs(
    comparers: Iterable[LevelComparer],
    left: ir.Table,
    right: ir.Table,
    *,
    true_pairs: ir.Table,
    max_pairs: int = 1_000_000_000,
) -> Weights:
    """Estimate all Weights for a set of LevelComparers using true pairs.

    The m parameters represent the proportion of record pairs
    that fall into each MatchLevel amongst truly matching pairs.
    This function estimates the m parameters using the provided true pairs.

    The u parameters represent the proportion of record pairs
    that fall into each MatchLevel amongst truly non-matching records.
    This function estimates the u parameters using random sampling.

    Parameters
    ----------
    comparers
        The comparers to train.
    left
        The left dataset.
    right
        The right dataset.
    true_pairs
        Record pairs that are true matches.
        This should be a table with columns `record_id_l` and `record_id_r`.
        Each row represents a pair of matching records.
        Non-matching pairs should not be included in this table.
    max_pairs
        The maximum number of pairs to sample.
        This is used for both the m and u estimates.

    Returns
    -------
    Weights
        The estimated weights for each comparer.
    """

    def f(comparer: LevelComparer) -> ComparerWeights:
        ms = _train_ms_from_pairs(comparer, true_pairs, max_pairs=max_pairs)
        us = _train_us_using_sampling(comparer, left, right, max_pairs=max_pairs)
        return make_weights(comparer, ms, us)

    return Weights(f(c) for c in comparers)


def train_using_labels(
    comparers: Iterable[LevelComparer],
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int = 1_000_000_000,
) -> Weights:
    """Estimate all Weights for a set of LevelComparers using labeled records.

    The m parameters represent the proportion of record pairs
    that fall into each MatchLevel amongst truly matching pairs.
    This function estimates the m parameters using the `label_true` columns
    in the input datasets.

    The u parameters represent the proportion of record pairs
    that fall into each MatchLevel amongst truly non-matching records.
    This function estimates the u parameters using random sampling.

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
        This is used for both the m and u estimates.

    Returns
    -------
    Weights
        The estimated weights for each comparer.
    """

    def f(comparer: LevelComparer) -> ComparerWeights:
        ms = _train_ms_from_labels(comparer, left, right, max_pairs=max_pairs)
        us = _train_us_using_sampling(comparer, left, right, max_pairs=max_pairs)
        return make_weights(comparer, ms, us)

    return Weights(f(c) for c in comparers)


def make_weights(
    comparer: LevelComparer, ms: list[float], us: list[float]
) -> ComparerWeights:
    levels = comparer.levels
    assert len(ms) == len(us) == len(levels)
    level_weights = [
        LevelWeights(level, m=m, u=u) for level, m, u in zip(levels, ms, us)
    ]
    level_weights = [lw for lw in level_weights if lw.name != "else"]
    return ComparerWeights(comparer.name, level_weights)
