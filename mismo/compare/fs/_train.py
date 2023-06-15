from __future__ import annotations

from mismo._dataset import PDatasetPair
from mismo._util import sample_table
from mismo.block._blocker import CartesianBlocker

from ._fs import Condition


def train_u(
    condition: Condition,
    dataset_pair: PDatasetPair,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> float:
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
        max_pairs = 1_000_000
    blocking = CartesianBlocker().block(dataset_pair)
    pairs = blocking.blocked_data
    n_pairs = min(max_pairs, pairs.count().execute())
    sample = sample_table(pairs, n_pairs, seed=seed)
    return condition.predicate(sample).fillna(False).cast(int).mean().execute()  # type: ignore # noqa: E501
