from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as it


def key_counts(t: it.Table, key) -> it.Table:
    """Count the number of occurrences of each join key in a table.

    This is useful for analyzing the skew of the join keys. For example,
    if you are joining on (last_name, city), there might be only 4 values
    for (hoessle, tinytown), but 10_000 values for (smith, new york city).
    If you blocked on this key, then the (smith, new york city) block
    would be on the order of 10_000 * 10_000 = 100_000_000 record pairs,
    which is likely too many for you to be able to compare.

    Returns a table with a column for each value in `key` and a column
    `n` with the count.

    This is slightly different from `t.group_by(key).count()`,
    because when joining, NULLs are *not* counted as a match.

    Parameters
    ----------
    t
        The table to count the join keys in
    key
        The join key(s) to count. This can be a string, a Deferred, or anything
        that can reference a column in a Table. Or an iterable of these.

    Returns
    -------
    it.Table
        A table with a column for each value in `key` and a column `n` with the count.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> from ibis import _
    >>> import mismo
    >>> records = [
    ...     ("a", 1),
    ...     ("b", 1),
    ...     ("b", 1),
    ...     ("c", 3),
    ...     ("b", 2),
    ...     ("c", 3),
    ...     (None, 4),
    ...     ("c", 3),
    ... ]
    >>> letters, nums = zip(*records)
    >>> t = ibis.memtable({"letter": letters, "num": nums})
    >>> mismo.block.key_counts(t, ("letter", _.num))
    ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
    ┃ letter ┃ num   ┃ n     ┃
    ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
    │ string │ int64 │ int64 │
    ├────────┼───────┼───────┤
    │ c      │     3 │     3 │
    │ b      │     1 │     2 │
    │ a      │     1 │     1 │
    │ b      │     2 │     1 │
    └────────┴───────┴───────┘
    """
    t = t.select(key)
    t = t.dropna(how="any")
    gb = t.group_by(t.columns).agg(n=_.count()).order_by(_.n.desc())
    return gb


def estimate_n_pairs(left: it.Table, right: it.Table, key) -> it.IntegerScalar:
    """Estimate the number of pairs that would be created by blocking.

    This uses `key_counts` to count the number of occurrences of each join key
    in each table, joins them to estimate the number of pairs in each block,
    then sums them up.

    This will be a slight overestimate, because a record pair might be in multiple
    blocks. That pair will be counted multiple times by this function,
    even though if you actually blocked on that key, the results would be
    de-duplicated.

    Parameters
    ----------
    left
        The left table
    right
        The right table
    key
        The join key(s) to count. This can be a string, a Deferred, or anything
        that can reference a column in a Table. Or an iterable of these.

    Returns
    -------
    The estimated number of pairs that would be created by blocking.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> from ibis import _
    >>> import mismo
    >>> records = [
    ...     ("a", 1),
    ...     ("b", 1),
    ...     ("b", 1),
    ...     ("c", 3),
    ...     ("b", 2),
    ...     ("c", 3),
    ...     (None, 4),
    ...     ("c", 3),
    ... ]
    >>> letters, nums = zip(*records)
    >>> t = ibis.memtable({"letter": letters, "num": nums})
    >>> mismo.block.key_counts(t, ("letter", _.num))
    ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
    ┃ letter ┃ num   ┃ n     ┃
    ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
    │ string │ int64 │ int64 │
    ├────────┼───────┼───────┤
    │ c      │     3 │     3 │
    │ b      │     1 │     2 │
    │ a      │     1 │     1 │
    │ b      │     2 │     1 │
    └────────┴───────┴───────┘

    If we joined this table with itself on this key, we would get
    9 + 4 + 1 + 1 = 15 pairs:

    >>> mismo.block.estimate_n_pairs(t, t, ("letter", _.num))
    15

    """
    kcl = key_counts(left, key)
    kcr = key_counts(right, key)
    k = [c for c in kcl.columns if c != "n"]
    j = ibis.join(kcl, kcr, k)
    return (j.n * j.n_right).sum()
