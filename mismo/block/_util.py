from __future__ import annotations

import warnings

import ibis
from ibis.expr import types as ir

from mismo.block._blocker import CrossBlocker
from mismo.block._core import fix_blocked_column_order


# TODO: could we come up with an algorithm that would allow for a random seed?
def sample_all_pairs(
    left: ir.Table,
    right: ir.Table,
    *,
    max_pairs: int | None = None,
) -> ir.Table:
    """Samples up to `max_pairs` from all possible pairs of records.

    Parameters
    ----------
    left :
        The left table.
    right :
        The right table.
    max_pairs :
        The maximum number of pairs to sample. If None, all possible pairs are sampled.

    Returns
    -------
    ir.Table
        A blocked table of pairs, in the same schema as from other blocking functions.
        All pairs will be unique.

    Examples
    --------
    >>> import ibis
    >>> from mismo import datasets, block
    >>> ibis.options.interactive = True
    >>> t, _labels = datasets.load_febrl1()
    >>> t.head(5)
    ┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━┳━━━┓
    ┃ record_id    ┃ given_name ┃ surname    ┃ street_number ┃ address_1         ┃ address_2 ┃ suburb    ┃ postcode ┃ state  ┃ … ┃
    ┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━╇━━━┩
    │ string       │ string     │ string     │ string        │ string            │ string    │ string    │ string   │ string │ … │
    ├──────────────┼────────────┼────────────┼───────────────┼───────────────────┼───────────┼───────────┼──────────┼────────┼───┤
    │ rec-0-dup-0  │ thomas     │ rokobaro   │ 12            │ herschell circuit │ killanrey │ lawrence  │ 2272     │ nsw    │ … │
    │ rec-0-org    │ flynn      │ rokobaro   │ 12            │ herschell circuit │ killarney │ lawrence  │ 2227     │ nsw    │ … │
    │ rec-1-dup-0  │ karli      │ alderson   │ 144           │ nulsen circuit    │ iowanna   │ tings lpa │ 3139     │ nsw    │ … │
    │ rec-1-org    │ karli      │ alderson   │ 144           │ nulsen circuit    │ iowanna   │ tingalpa  │ 3139     │ nsw    │ … │
    │ rec-10-dup-0 │ kayla      │ harrington │ NULL          │ maltby circuit    │ coaling   │ coolaroo  │ 3465     │ nsw    │ … │
    └──────────────┴────────────┴────────────┴───────────────┴───────────────────┴───────────┴───────────┴──────────┴────────┴───┘
    >>> block.sample_all_pairs(t, t, max_pairs = 7)
    ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━┓
    ┃ record_id_l   ┃ record_id_r   ┃ address_1_l       ┃ address_1_r      ┃ address_2_l                ┃ address_2_r                ┃ … ┃
    ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━┩
    │ string        │ string        │ string            │ string           │ string                     │ string                     │ … │
    ├───────────────┼───────────────┼───────────────────┼──────────────────┼────────────────────────────┼────────────────────────────┼───┤
    │ rec-179-org   │ rec-439-org   │ coghill close     │ clarnette place  │ bay city plaza             │ backwoodlands              │ … │
    │ rec-23-org    │ rec-290-dup-0 │ dilboong place    │ tillyard drive   │ rosetta village            │ willow wood                │ … │
    │ rec-376-dup-0 │ rec-245-org   │ findlay wtreet    │ discovery street │ booroopki park rmb 596     │ rockview                   │ … │
    │ rec-454-dup-0 │ rec-365-dup-0 │ tarana street     │ canber ra avenue │ aprt 5x9                   │ aranmor                    │ … │
    │ rec-498-dup-0 │ rec-117-org   │ oyster farm       │ augustus close   │ basedow street             │ riverbank offices          │ … │
    │ rec-343-dup-0 │ rec-39-dup-0  │ vincent place     │ coventry close   │ NULL                       │ cressbrook                 │ … │
    │ rec-494-org   │ rec-307-dup-0 │ mcmillan crescent │ birrigaisquare   │ arcadia                    │ arabian stud ielkette park │ … │
    └───────────────┴───────────────┴───────────────────┴──────────────────┴────────────────────────────┴────────────────────────────┴───┘
    """  # noqa: E501
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

    if max_pairs is None:
        return CrossBlocker()(left, right)

    # ibis can't handle ibis.range(0), that's a bug with them
    if n_pairs == 0:
        return CrossBlocker()(left, right).limit(0)

    def make_pair_ids():
        pair_ids = ibis.range(n_pairs).unnest().as_table()
        return pair_ids.select(
            __left_id=(ibis.random() * left.count()).floor().cast("int64"),
            __right_id=(ibis.random() * right.count()).floor().cast("int64"),
        )

    # This iterative method is the best I could come up with to avoid duplicates.
    # A better method might be possible.
    # At first I tried ibis.range(n_pairs * 2)....distinct().limit(n_pairs)
    # but choosing the safety margin (here it is 2) is tricky.
    # The closer n_pairs gets to n_possible_pairs, the higher the safety margin
    # needs to be.
    # But you are doing unneeded computation if you set it higher than it needs
    # to be.
    # For example if we have 10 records, and max_pairs is 99, then we need
    # to do really large samples in order to get to the 99 unique pairs.
    # On the other hand, if we have 10k records and max_pairs is only 10,
    # then we could get away with doing a sample of 20 pairs and we would
    # probably get all unique pairs.
    pair_ids = make_pair_ids().distinct().cache()
    while pair_ids.count().execute() < n_pairs:
        pair_ids = pair_ids.union(make_pair_ids()).distinct().limit(n_pairs).cache()

    right = right.view()
    left = left.rename("{name}_l").mutate(__left_id=ibis.row_number())
    right = right.rename("{name}_r").mutate(__right_id=ibis.row_number())
    result = (
        pair_ids.left_join(left, "__left_id")
        .left_join(right, "__right_id")
        .drop("__left_id", "__right_id", "__left_id_right", "__right_id_right")
    )
    return fix_blocked_column_order(result)
