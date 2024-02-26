from __future__ import annotations

from typing import Any

import ibis
from ibis import _
from ibis.expr import types as it


def key_counts(left: it.Table, right_or_key: it.Table | Any, key=None, /) -> it.Table:
    """Count the number of occurrences of each join key in a table or pair of tables.

    Used as `key_counts(t, key)` or `key_counts(t1, t2, key)`.

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
    left
        The left table to count the join keys in
    right_or_key
        The right table to count the join keys in, or the join key(s) to count.
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

    If we joined t with itself using this blocker, these are the sizes of the
    blocks we would get:

    >>> counts = mismo.block.key_counts(t, t, ("letter", _.num))
    >>> counts
    ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
    ┃ letter ┃ num   ┃ n     ┃
    ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
    │ string │ int64 │ int64 │
    ├────────┼───────┼───────┤
    │ c      │     3 │     9 │
    │ b      │     1 │     4 │
    │ a      │     1 │     1 │
    │ b      │     2 │     1 │
    └────────┴───────┴───────┘

    The total number of pairs that would be generated is easy to find:

    >>> counts.n.sum()
    15
    """
    if key is None:
        key = right_or_key
        return _key_counts(left, key)
    else:
        kcl = _key_counts(left, key)
        kcr = _key_counts(right_or_key, key)
        k = [c for c in kcl.columns if c != "n"]
        j = ibis.join(kcl, kcr, k)
        j = j.mutate(n=_.n * _.n_right).drop("n_right")
        j = j.order_by(_.n.desc())
        return j


def _key_counts(t: it.Table, key) -> it.Table:
    if key is None:
        raise TypeError("key cannot be None")
    t = t.select(key)
    t = t.dropna(how="any")
    return t.group_by(t.columns).agg(n=_.count()).order_by(_.n.desc())
