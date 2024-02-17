from __future__ import annotations

from typing import Iterable

from ibis.expr.types import IntegerColumn, StringColumn, Table

from mismo._util import sample_table
from mismo.block import block
from mismo.compare import LevelComparer, compare

from ._weights import ComparisonWeights, LevelWeights, Weights


def all_possible_pairs(
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> Table:
    """Blocks together all possible pairs of records."""
    pairs = block(left, right, True, on_slow="ignore")
    n_pairs = _min_ignore_None(pairs.count().execute(), max_pairs)
    return sample_table(pairs, n_pairs, seed=seed)


def true_pairs_from_labels(left: Table, right: Table) -> Table:
    if "label_true" not in left.columns:
        raise ValueError(
            "Left dataset must have a label_true column. Found: {left.columns}"
        )
    if "label_true" not in right.columns:
        raise ValueError(
            "Right dataset must have a label_true column. Found: {right.columns}"
        )
    return block(left, right, "label_true")


def level_proportions(
    comparer: LevelComparer, labels: IntegerColumn | StringColumn
) -> list[float]:
    """
    Return the proportion of labels that fall into each Comparison level.
    """
    vc = labels.name("level").value_counts().rename(n="level_count")
    vc_df = vc.to_pandas().set_index("level")
    # If we didn't see a level, that won't be present in the value_counts table.
    # Add it in, with a count of 1 to regularaize it.
    # If a level shows shows up 0 times among nonmatches, this would lead to an odds
    # of M/0 = infinity.
    # If it shows up 0 times among matches, this would lead to an odds of 0/M = 0.
    # To avoid this, for any levels that we didn't see, we pretend we saw them once.
    vc_df = vc_df.reindex([lev["name"] for lev in comparer], fill_value=1)
    vc_df["pct"] = vc_df["n"] / vc_df["n"].sum()
    vc_dict = vc_df["pct"].to_dict()
    if isinstance(labels, StringColumn):
        name_to_i = {lev["name"]: i for i, lev in enumerate(comparer)}
        vc_dict = {name_to_i[name]: v for name, v in vc_dict.items()}
    return [vc_dict[i] for i in range(len(comparer))]


def train_us_using_sampling(
    comparer: LevelComparer,
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
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
    if max_pairs is None:
        max_pairs = 1_000_000_000
    sample = all_possible_pairs(left, right, max_pairs=max_pairs, seed=seed)
    labels = compare(sample, comparer)[comparer.name]
    return level_proportions(comparer, labels)


def train_ms_from_labels(
    comparer: LevelComparer,
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
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
    pairs = true_pairs_from_labels(left, right)
    if max_pairs is None:
        max_pairs = 1_000_000_000
    n_pairs = min(pairs.count().execute(), max_pairs)
    sample = sample_table(pairs, n_pairs, seed=seed)
    labels = compare(sample, comparer)[comparer.name]
    return level_proportions(comparer, labels)


def _train_using_labels(
    comparer: LevelComparer,
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> ComparisonWeights:
    ms = train_ms_from_labels(comparer, left, right, max_pairs=max_pairs, seed=seed)
    us = train_us_using_sampling(comparer, left, right, max_pairs=max_pairs, seed=seed)
    return make_weights(comparer, ms, us)


def train_using_labels(
    comparers: Iterable[LevelComparer],
    left: Table,
    right: Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> Weights:
    """Estimate all Weights for a set of LevelComparers using labeled data."""
    return Weights(
        [
            _train_using_labels(c, left, right, max_pairs=max_pairs, seed=seed)
            for c in comparers
        ]
    )


def make_weights(comparer: LevelComparer, ms: list[float], us: list[float]):
    assert len(ms) == len(us) == len(comparer)
    level_weights = [
        LevelWeights(level["name"], m=m, u=u) for m, u, level in zip(ms, us, comparer)
    ]
    level_weights = [lw for lw in level_weights if lw.name != "else"]
    return ComparisonWeights(comparer.name, level_weights)


def _min_ignore_None(*args):
    return min(*(a for a in args if a is not None))
