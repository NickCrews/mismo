from __future__ import annotations

import warnings

from ibis.expr import types as ir

from mismo._util import sample_table
from mismo.block import block_one


def sample_all_pairs(
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int | None = None,
    seed: int | None = None,
) -> ir.Table:
    """Samples up to `max_pairs` from all possible pairs of records."""
    left = left.cache()
    right = right.cache()
    n_possible_pairs = left.count().execute() * right.count().execute()
    n_pairs = (
        n_possible_pairs if max_pairs is None else min(n_possible_pairs, max_pairs)
    )
    if n_pairs > 100_000_000:
        msg = f"{n_pairs:,}" if n_pairs != n_possible_pairs else "all"
        warnings.warn(
            f"Sampling {msg} pairs from {n_possible_pairs:,} possible pairs."
            " This may be slow. Consider setting max_pairs to a smaller value."
        )
    pairs = block_one(left, right, True, on_slow="ignore")
    return sample_table(pairs, n_pairs, seed=seed)
