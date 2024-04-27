from __future__ import annotations

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
    pairs = block_one(left, right, True, on_slow="ignore")
    n_pairs = _min_ignore_None(pairs.count().execute(), max_pairs)
    return sample_table(pairs, n_pairs, seed=seed)


def _min_ignore_None(*args):
    return min(*(a for a in args if a is not None))
