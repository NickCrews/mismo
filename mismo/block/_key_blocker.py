from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Callable, Literal, NamedTuple

import ibis
from ibis import Deferred, _
from ibis.expr import types as ir

from mismo import _util
from mismo.block._core import block_on_id_pairs, join

if TYPE_CHECKING:
    import altair as alt


class _HistSpec(NamedTuple):
    n_title: str
    chart_title: str
    chart_subtitle: str


_KEY_COUNTS_SPEC = _HistSpec(
    "Number of Records", "Number of Records by Key", "{n_total:,} Total Records"
)
_PAIR_COUNTS_SPEC = _HistSpec(
    "Number of Pairs", "Number of Pairs by Key", "{n_total:,} Total Pairs"
)


class CountsTable(ibis.Table):
    """A table with at least an Integer column named `n`.

    There will also be variable number of other columns that act as identifiers.

    You won't create this directly, it will be returned to you
    from eg [KeyBlocker.key_counts()][mismo.block.KeyBlocker.key_counts] or
    [KeyBlocker.pair_counts()][mismo.block.KeyBlocker.pair_counts].
    """

    n: ir.IntegerColumn
    """The column containing the count."""

    def __init__(self, t: ibis.Table, *, hist_spec: _HistSpec) -> None:
        object.__setattr__(self, "_t", t)
        object.__setattr__(self, "_hist_spec", hist_spec)

    def __getattr__(self, key: str):
        return getattr(self._t, key)

    @property
    @functools.cache
    def n_total(self) -> int:
        """n.sum(), just here for convenience."""
        return int(self.n.sum().execute())

    def chart(self, *, n_most_common: int = 50, n_least_common: int = 10) -> alt.Chart:
        """Create an Altair histogram chart."""
        return _counts_chart(
            self,
            hist_spec=self._hist_spec,
            n_most_common=n_most_common,
            n_least_common=n_least_common,
        )


def _counts_chart(
    counts: CountsTable,
    *,
    hist_spec: _HistSpec,
    n_most_common: int,
    n_least_common: int,
):
    import altair as alt

    key_cols = [c for c in counts.columns if c != "n"]
    val = "(" + ibis.literal(", ").join(counts[c] for c in key_cols) + ")"
    key_and_n = counts.mutate(val.name("key"), "n")
    key_and_n = key_and_n.filter(_.n > 0)
    most_common = key_and_n.order_by(_.n.desc()).head(n_most_common)
    least_common = key_and_n.order_by(_.n.asc()).head(n_least_common)
    subset = ibis.union(most_common, least_common, distinct=False).cache()
    n_keys_shown = subset.count().execute()
    n_keys_total = key_and_n.count().execute()
    key_title = "(" + ", ".join(key_cols) + ")"
    chart = (
        alt.Chart(subset)
        .properties(
            title=alt.TitleParams(
                hist_spec.chart_title,
                subtitle=[
                    hist_spec.chart_subtitle.format(n_total=counts.n_total),
                    f"Showing the {n_keys_shown:,} most and least common keys out of {n_keys_total:,}",  # noqa: E501
                ],
                anchor="middle",
            ),
            width=alt.Step(10),
        )
        .mark_bar()
        .encode(
            alt.X("key:O", title=key_title, sort="-y"),
            alt.Y(
                "n:Q",
                title=hist_spec.n_title,
                # scale=alt.Scale(type="log", domainMin=10),
            ),
            tooltip=[
                alt.Tooltip("n:Q", title=hist_spec.n_title, format=","),
                *[alt.Tooltip(col) for col in key_cols],
            ],
        )
    )
    return chart


class KeyBlocker:
    """Blocks records together wherever they share a key, eg "emails match."

    This is one of the most basic blocking rules, used very often in record linkage.
    This is what is used in `splink`.

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> t = mismo.playdata.load_patents()["record_id", "name", "latitude"]
    >>> t.head()
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
    ┃ record_id ┃ name                         ┃ latitude ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
    │ int64     │ string                       │ float64  │
    ├───────────┼──────────────────────────────┼──────────┤
    │      2909 │ * AGILENT TECHNOLOGIES, INC. │     0.00 │
    │      3574 │ * AKZO NOBEL N.V.            │     0.00 │
    │      3575 │ * AKZO NOBEL NV              │     0.00 │
    │      3779 │ * ALCATEL N.V.               │    52.35 │
    │      3780 │ * ALCATEL N.V.               │    52.35 │
    └───────────┴──────────────────────────────┴──────────┘

    Block the table with itself wherever the names match:

    >>> blocker = mismo.block.KeyBlocker("name")
    >>> blocker(t, t).order_by("record_id_l", "record_id_r").head()  # doctest: +SKIP
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l         ┃ name_r         ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string         │ string         │
    ├─────────────┼─────────────┼────────────┼────────────┼────────────────┼────────────────┤
    │        3779 │        3780 │      52.35 │  52.350000 │ * ALCATEL N.V. │ * ALCATEL N.V. │
    │        3779 │        3782 │      52.35 │   0.000000 │ * ALCATEL N.V. │ * ALCATEL N.V. │
    │        3780 │        3782 │      52.35 │   0.000000 │ * ALCATEL N.V. │ * ALCATEL N.V. │
    │       25388 │     7651559 │       0.00 │  50.966667 │ DSM N.V.       │ DSM N.V.       │
    │       25388 │     7651560 │       0.00 │  52.500000 │ DSM N.V.       │ DSM N.V.       │
    └─────────────┴─────────────┴────────────┴────────────┴────────────────┴────────────────┘

    Arbitrary blocking keys are supported. For example, block the table wherever

    - the first 5 characters of the name in uppercase, are the same
        AND
    - the latitudes, rounded to 1 decimal place, are the same

    >>> blocker = mismo.block.KeyBlocker(_["name"][:5].upper(), _.latitude.round(1))
    >>> blocker(t, t).order_by("record_id_l", "record_id_r").head()  # doctest: +SKIP
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l              ┃ name_r              ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string              │ string              │
    ├─────────────┼─────────────┼────────────┼────────────┼─────────────────────┼─────────────────────┤
    │        3574 │        3575 │       0.00 │       0.00 │ * AKZO NOBEL N.V.   │ * AKZO NOBEL NV     │
    │        3779 │        3780 │      52.35 │      52.35 │ * ALCATEL N.V.      │ * ALCATEL N.V.      │
    │       15041 │       15042 │       0.00 │       0.00 │ * CANON EUROPA N.V  │ * CANON EUROPA N.V. │
    │       15041 │       15043 │       0.00 │       0.00 │ * CANON EUROPA N.V  │ * CANON EUROPA NV   │
    │       15042 │       15043 │       0.00 │       0.00 │ * CANON EUROPA N.V. │ * CANON EUROPA NV   │
    └─────────────┴─────────────┴────────────┴────────────┴─────────────────────┴─────────────────────┘

    We can even block on arrays! For example, first let's split each name into
    significant tokens:

    >>> tokens = _.name.upper().split(" ").filter(lambda x: x.length() > 4)
    >>> t.select(tokens)
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ ArrayFilter(StringSplit(Uppercase(name), ' '), Greater(StringLength(x), 4)) ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                                                               │
    ├─────────────────────────────────────────────────────────────────────────────┤
    │ ['AGILENT', 'TECHNOLOGIES,']                                                │
    │ ['NOBEL']                                                                   │
    │ ['NOBEL']                                                                   │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ []                                                                          │
    │ …                                                                           │
    └─────────────────────────────────────────────────────────────────────────────┘

    Now, block the tables together wherever two records share a token.
    Note that this blocked `* SCHLUMBERGER LIMITED` with `* SCHLUMBERGER TECHNOLOGY BV`.
    because they both share the `SCHLUMBERGER` token.

    >>> blocker = mismo.block.KeyBlocker(tokens.unnest())
    >>> blocker(t, t).filter(_.name_l != _.name_r).order_by("record_id_l", "record_id_r").head()  # doctest: +SKIP
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                                                     ┃ name_r                                                     ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                                                     │ string                                                     │
    ├─────────────┼─────────────┼────────────┼────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┤
    │        2909 │    13390969 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Inc. Netherlands B.V  │
    │        2909 │    13390970 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Inc. Netherlands B.V. │
    │        2909 │    13391015 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherland B.V.       │
    │        2909 │    13391055 │        0.0 │      52.50 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherlands, B.V.     │
    │        2909 │    13391056 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherlands, B.V.     │
    └─────────────┴─────────────┴────────────┴────────────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
    """  # noqa: E501

    def __init__(
        self,
        *keys: str
        | Deferred
        | Callable[[ibis.Table], ir.Column | str | Deferred]
        | tuple[
            str | Deferred | Callable[[ibis.Table], ir.Column | str | Deferred],
            str | Deferred | Callable[[ibis.Table], ir.Column | str | Deferred],
        ]
        | Callable[[ibis.Table, ibis.Table], tuple[ir.Column, ir.Column]],
        name: str | None = None,
    ) -> None:
        """Create a KeyBlocker.

        Parameters
        ----------
        keys
            The keys to block on.
            The tables will be blocked together wherever they share ALL the keys.
            Each key can be any of the following:

            - A string, which is interpreted as the name of a column in both tables.
              eg "price" is equivalent to `left.price == right.price`
            - A Deferred, which is used to reference a column in a table.
              eg `_.price.fill_null(0)` is equivalent to
              `left.price.fill_null(0) == right.price.fill_null(0)`
            - A Callable that takes a table and returns a Column.
            - A 2-tuple of the above, where the first element describes the key in the
              left table and the second element describes the key in the right table.
              eg `("first_name", _.GivenName.upper()")` is equivalent to
              `left.first_name == right.GivenName.upper()`
              This is useful when the keys have different names in the two tables.
            - A callable that takes the left and right tables and returns a tuple
              of columns. Left and right will be joined where the columns are equal.
        """  # noqa: E501
        self.keys = keys
        self._name = name if name is not None else _util.get_name(self.keys)

    @property
    def name(self) -> str:
        """The name of the KeyBlocker."""
        return self._name

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
        **kwargs,
    ):
        j = join(left, right, *self.keys, task=task, on_slow="ignore", **kwargs)
        id_pairs = j.select("record_id_l", "record_id_r").distinct()
        return block_on_id_pairs(left, right, id_pairs)

    def key_counts(self, t: ir.Table, /) -> CountsTable:
        """Count the join keys in a table.

        This is very similar to `t.group_by(key).count()`, except that counts NULLs,
        whereas this method does not count NULLs since they are not
        counted as a match during joins.

        This is useful for analyzing the skew of join keys.
        For example, if you are joining on (surname, city),
        there might be only 4 values for (hoessle, tinytown),
        which would lead to a block of 4 * 4 = 16 record pairs.

        On the other hand, there could be 10_000 values for (smith, new york city).
        This would lead to 10_000 * 10_000 = 100_000_000 record pairs,
        which is likely too many for you to be able to compare.

        Parameters
        ----------
        t
            The table of records.

        Returns
        -------
        CountsTable
            Will have column(s) for each `key` and a column `n` with the count.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
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
        >>> blocker = mismo.block.KeyBlocker("letter", "num")

        Note how the (None, 4) record is not counted,
        since NULLs are not counted as a match during a join.

        >>> counts = blocker.key_counts(t)
        >>> counts.order_by("letter", "num")
        ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
        ┃ letter ┃ num   ┃ n     ┃
        ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
        │ string │ int64 │ int64 │
        ├────────┼───────┼───────┤
        │ a      │     1 │     1 │
        │ b      │     1 │     2 │
        │ b      │     2 │     1 │
        │ c      │     3 │     3 │
        └────────┴───────┴───────┘

        The returned CountsTable is a subclass of an Ibis Table
        with a special `n_total` property for convenience:

        >>> counts.n_total
        7
        >>> isinstance(counts, ibis.Table)
        True
        """
        t = t.select(self.keys)
        t = t.drop_null(how="any")
        t = t.group_by(t.columns).agg(n=_.count()).order_by(_.n.desc())
        return CountsTable(t, hist_spec=_KEY_COUNTS_SPEC)

    def pair_counts(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ) -> CountsTable:
        """Count the number of pairs that would be generated by each key.

        If you were to use this blocker to join `left` with `right`,
        how many pairs would be generated for each key?

        This is useful for analyzing the skew of join keys.
        For example, if you are joining on (surname, city),
        there might be only 4 values for (hoessle, tinytown),
        which would lead to a block of 4 * 4 = 16 record pairs.

        On the other hand, there could be 10_000 values for (smith, new york city).
        This would lead to 10_000 * 10_000 = 100_000_000 record pairs,
        which is likely too many for you to be able to compare.

        Parameters
        ----------
        left
            The left table.
        right
            The right table.
        task
            The task to count pairs for.

            - "link": each key results in n_left * n_right pairs
            - "dedupe": each key results in n_left * (n_right - 1) / 2 pairs
               since we will only generate pair (A, B), not also (B, A).
            - None: inferred from the input tables: if `left is right`, then "dedupe",
              otherwise "link".

        Returns
        -------
        CountsTable
            Will have column(s) for each `key` and a column `n` with the count.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
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
        >>> blocker = mismo.block.KeyBlocker("letter", "num")

        If we joined t with itself using this blocker in a link task,
        we would end up with

        - 9 pairs in the (c, 3) block due to pairs (3,3), (3, 6), (3, 8), (6, 3), (6, 6), (6, 8), (8, 3), (8, 6), and (8, 8)
        - 4 pairs in the (b, 1) block due to pairs (1, 1), (1, 2), (2, 1), and (2, 2)
        - 1 pairs in the (a, 1) block due to pair (0, 0)
        - 1 pairs in the (b, 2) block due to pair (4, 4)

        >>> counts = blocker.pair_counts(t, t, task="link").order_by("letter", "num")
        >>> counts
        ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
        ┃ letter ┃ num   ┃ n     ┃
        ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
        │ string │ int64 │ int64 │
        ├────────┼───────┼───────┤
        │ a      │     1 │     1 │
        │ b      │     1 │     4 │
        │ b      │     2 │     1 │
        │ c      │     3 │     9 │
        └────────┴───────┴───────┘

        If we joined t with itself using this blocker in a dedupe task,
        we would end up with

        - 3 pairs in the (c, 3) block due to pairs (3, 6), (3, 8), and (6, 8)
        - 1 pairs in the (b, 1) block due to pairs (1, 2)
        - 0 pairs in the (a, 1) block due to record 0 not getting blocked with itself
        - 0 pairs in the (b, 2) block due to record 4 not getting blocked with itself

        >>> counts = blocker.pair_counts(t, t).order_by("letter", "num")
        >>> counts
        ┏━━━━━━━━┳━━━━━━━┳━━━━━━━┓
        ┃ letter ┃ num   ┃ n     ┃
        ┡━━━━━━━━╇━━━━━━━╇━━━━━━━┩
        │ string │ int64 │ int64 │
        ├────────┼───────┼───────┤
        │ a      │     1 │     0 │
        │ b      │     1 │     1 │
        │ b      │     2 │     0 │
        │ c      │     3 │     3 │
        └────────┴───────┴───────┘

        The returned CountsTable is a subclass of an Ibis Table
        with a special `n_total` property for convenience:

        >>> counts.n_total
        4
        >>> isinstance(counts, ibis.Table)
        True
        """  # noqa: E501
        if task is None:
            task = "dedupe" if left is right else "link"
        kcl = self.key_counts(left)
        kcr = self.key_counts(right)
        if task == "dedupe":
            by_key = kcl.mutate(n=(_.n * (_.n - 1) / 2).cast(int))
        else:
            k = [c for c in kcl.columns if c != "n"]
            by_key = ibis.join(kcl, kcr, k).mutate(n=_.n * _.n_right).drop("n_right")
        by_key = by_key.order_by(_.n.desc())
        return CountsTable(by_key, hist_spec=_PAIR_COUNTS_SPEC)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"
