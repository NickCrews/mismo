from __future__ import annotations

import warnings

import ibis

from mismo.types import LinksTable


# TODO: could we come up with an algorithm that would allow for a random seed?
def sample_all_links(
    left: ibis.Table,
    right: ibis.Table,
    *,
    max_pairs: int | None = None,
) -> LinksTable:
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
    A [LinksTable][mismo.LinksTable] with just record_id_l and record_id_r.
    All pairs will be unique.

    Examples
    --------
    >>> import ibis
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> t, _labels = mismo.playdata.load_febrl1()
    >>> t.head(5)
    ┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━┳━━━┓
    ┃ record_id    ┃ given_name ┃ surname    ┃ street_number ┃ address_1         ┃ … ┃
    ┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━╇━━━┩
    │ string       │ string     │ string     │ string        │ string            │ … │
    ├──────────────┼────────────┼────────────┼───────────────┼───────────────────┼───┤
    │ rec-0-dup-0  │ thomas     │ rokobaro   │ 12            │ herschell circuit │ … │
    │ rec-0-org    │ flynn      │ rokobaro   │ 12            │ herschell circuit │ … │
    │ rec-1-dup-0  │ karli      │ alderson   │ 144           │ nulsen circuit    │ … │
    │ rec-1-org    │ karli      │ alderson   │ 144           │ nulsen circuit    │ … │
    │ rec-10-dup-0 │ kayla      │ harrington │ NULL          │ maltby circuit    │ … │
    └──────────────┴────────────┴────────────┴───────────────┴───────────────────┴───┘
    >>> mismo.linkage.sample_all_links(t, t, max_pairs=7)  # doctest: +SKIP
    ┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
    ┃ record_id_l   ┃ record_id_r   ┃
    ┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
    │ string        │ string        │
    ├───────────────┼───────────────┤
    │ rec-226-dup-0 │ rec-396-org   │
    │ rec-232-dup-0 │ rec-402-dup-0 │
    │ rec-259-dup-0 │ rec-61-org    │
    │ rec-293-dup-0 │ rec-41-dup-0  │
    │ rec-448-org   │ rec-25-org    │
    └───────────────┴───────────────┘
    """  # noqa: E501
    left = left.cache()
    right = right.cache()
    n_possible_pairs = int(left.count().execute() * right.count().execute())
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
        return LinksTable.from_join_condition(left, right, True)

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
    left_ids = left.select(record_id_l="record_id").mutate(__left_id=ibis.row_number())
    right_ids = right.select(record_id_r="record_id").mutate(
        __right_id=ibis.row_number()
    )
    raw_links = (
        pair_ids.left_join(left_ids, "__left_id")
        .left_join(right_ids, "__right_id")
        .drop("__left_id", "__right_id", "__left_id_right", "__right_id_right")
    )
    return LinksTable(raw_links, left=left, right=right)
