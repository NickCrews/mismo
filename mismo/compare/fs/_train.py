from __future__ import annotations

from dataclasses import replace

from ibis.expr.types import BooleanColumn, Table

from mismo._dataset import PDatasetPair
from mismo._util import sample_table
from mismo.block._blocker import CartesianBlocker, FunctionBlocker

from ._base import Comparison, Weights


def all_possible_pairs(
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int = 1_000_000_000,
    seed: int | None = None,
) -> Table:
    pairs = CartesianBlocker().block(dataset_pair).blocked_data
    n_pairs = min(max_pairs, pairs.count().execute())
    return sample_table(pairs, n_pairs, seed=seed)


def all_true_pairs_from_labels(
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int = 1_000_000_000,
) -> Table:
    left, right = dataset_pair
    if left.true_label_column is None or right.true_label_column is None:
        raise ValueError(
            "Both datasets must have a true_label_column to train m from labels"
        )

    def block_condition(le: Table, r: Table) -> BooleanColumn:
        return le[left.true_label_column] == r[right.true_label_column]  # type: ignore

    true_matches = FunctionBlocker(block_condition).block(dataset_pair).blocked_data
    n_pairs = min(max_pairs, true_matches.count().execute())
    return sample_table(true_matches, n_pairs)


def level_proportions(
    comparison: Comparison,
    blocked_data: Table,
) -> list[float]:
    """
    For each comparison level, return the proportion of pairs that fall into that level.
    """
    labels = comparison.label_pairs(blocked_data)
    vc = labels.name("level").value_counts()
    vc = vc.mutate(pct=vc.level_count / vc.level_count.sum())
    vc = vc.order_by("level")
    vc = vc.dropna(subset="level")
    return vc.pct.execute().tolist()


def train_us_using_sampling(
    comparison: Comparison,
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int = 1_000_000_000,
    seed: int | None = None,
) -> list[float]:
    """Estimate the u weight using random sampling.

    This is from splink's `estimate_u_using_random_sampling()`

    The u parameters represent the proportion of record comparisons
    that fall into each comparison level amongst truly non-matching records.

    This procedure takes a sample of the data and generates the cartesian
    product of pairwise record comparisons amongst the sampled records.
    The validity of the u values rests on the assumption that the resultant
    pairwise comparisons are non-matches (or at least, they are very unlikely
    to be matches). For large datasets, this is typically true.

    The results of estimate_u_using_random_sampling, and therefore an
    entire splink model, can be made reproducible by setting the seed
    parameter. Setting the seed will have performance implications as
    additional processing is required.

    Args:
        max_pairs:
            The maximum number of pairwise record comparisons to
            sample. Larger will give more accurate estimates
            but lead to longer runtimes.  In our experience at least 1e9 (one billion)
            gives best results but can take a long time to compute. 1e7 (ten million)
            is often adequate whilst testing different model specifications, before
            the final model is estimated.
    """
    sample = all_possible_pairs(dataset_pair, max_pairs=max_pairs, seed=seed)
    return level_proportions(comparison, sample)


def train_ms_from_labels(
    comparison: Comparison,
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int = 1_000_000_000,
) -> list[float]:
    """Using the true labels in the dataset, estimate the m weight.

    The m parameter represent the proportion of record pairs
    that fall into each comparison level amongst truly matching pairs.

    The ground truth column is used to generate pairwise record
    comparisons which are then assumed to be matches.

    For example, if the entity being matched is persons, and your
    input dataset(s) contain social security number, this could be
    used to estimate the m values for the model.

    Note that this column does not need to be fully populated.
    A common case is where a unique identifier such as social
    security number is only partially populated.
    When NULL values are encountered in the ground truth column,
    that record is simply ignored.
    """
    sample = all_true_pairs_from_labels(dataset_pair, max_pairs=max_pairs)
    return level_proportions(comparison, sample)


def train(
    comparison: Comparison,
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int = 1_000_000_000,
    seed: int | None = None,
) -> Comparison:
    """Train the weights of a Comparison."""
    ms = train_ms_from_labels(comparison, dataset_pair, max_pairs=max_pairs)
    us = train_us_using_sampling(
        comparison, dataset_pair, max_pairs=max_pairs, seed=seed
    )
    new_levels = [
        level.set_weights(Weights(m=m, u=u))
        for level, m, u in zip(comparison.levels, ms, us)
    ]
    return replace(comparison, levels=new_levels)
