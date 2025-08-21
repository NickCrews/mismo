from __future__ import annotations

from typing import Any, Callable, Iterable, Literal

import ibis
from ibis import Deferred, _
from ibis.expr import types as ir

from mismo import _resolve, _util
from mismo._counts_table import KeyCountsTable, PairCountsTable
from mismo.linkage import Linkage
from mismo.linker._common import Linker, infer_task
from mismo.types import LinksTable


class KeyLinker(Linker):
    """A [Linker][mismo.Linker] that links records wherever they share a key, eg "emails match."

    This is one of the most basic blocking rules, used very often in record linkage.
    This is what is used in `splink`.

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> linkage = mismo.playdata.load_patents()
    >>> t = linkage.left.select("record_id", "name", "latitude")
    >>> t.head()
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
    ┃ record_id ┃ name                         ┃ latitude ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
    │ uint32    │ string                       │ float64  │
    ├───────────┼──────────────────────────────┼──────────┤
    │      2909 │ * AGILENT TECHNOLOGIES, INC. │     0.00 │
    │      3574 │ * AKZO NOBEL N.V.            │     0.00 │
    │      3575 │ * AKZO NOBEL NV              │     0.00 │
    │      3779 │ * ALCATEL N.V.               │    52.35 │
    │      3780 │ * ALCATEL N.V.               │    52.35 │
    └───────────┴──────────────────────────────┴──────────┘

    Block the table with itself wherever the names match:

    >>> linker = mismo.KeyLinker("name")
    >>> linker(t, t).links.order_by(
    ...     "record_id_l", "record_id_r"
    ... ).head()  # doctest: +SKIP
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

    >>> linker = mismo.KeyLinker((_["name"][:5].upper(), _.latitude.round(1)))
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
    >>> t.select(tokens.name("tokens"))
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ tokens                       ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                │
    ├──────────────────────────────┤
    │ ['AGILENT', 'TECHNOLOGIES,'] │
    │ ['NOBEL']                    │
    │ ['NOBEL']                    │
    │ ['ALCATEL']                  │
    │ ['ALCATEL']                  │
    │ ['ALCATEL']                  │
    │ ['CANON', 'EUROPA']          │
    │ ['CANON', 'EUROPA']          │
    │ ['CANON', 'EUROPA']          │
    │ []                           │
    │ …                            │
    └──────────────────────────────┘

    Now, block the tables together wherever two records share a token.
    Note that this blocked `* SCHLUMBERGER LIMITED` with `* SCHLUMBERGER TECHNOLOGY BV`.
    because they both share the `SCHLUMBERGER` token.

    >>> linker = mismo.KeyLinker(tokens.unnest())
    >>> linker(t, t).links.filter(_.name_l != _.name_r).order_by(
    ...     "record_id_l", "record_id_r"
    ... ).head()  # doctest: +SKIP
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
        keys: str
        | ir.Value
        | Deferred
        | Callable[
            [ibis.Table, ibis.Table],
            tuple[ir.Value | ir.Column, ir.Value | ir.Column],
        ]
        | Iterable[
            str
            | ir.Value
            | Deferred
            | Callable[[ibis.Table], ir.Value | ir.Column | str | Deferred]
            | tuple[
                str
                | Deferred
                | Callable[[ibis.Table], ir.Value | ir.Column | str | Deferred],
                str
                | Deferred
                | Callable[[ibis.Table], ir.Value | ir.Column | str | Deferred],
            ]
            | Callable[
                [ibis.Table, ibis.Table],
                tuple[ir.Value | ir.Column, ir.Value | ir.Column],
            ]
        ],
        *,
        max_pairs: int | None = None,
        task: Literal["dedupe", "lookup", "link"] | None = None,
    ) -> None:
        """Create a KeyBlocker.

        Parameters
        ----------
        keys:
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
        max_pairs:
            The maximum number of pairs to generate for each key.
            This is to reduce the impact of very common keys.
            For example, if you are linking people, the name "John Smith" might be
            very common, appearing 5000 times in both left and right.
            This name alone would generate 5000 * 5000 = 25 million pairs,
            which might be too computationally expensive.
            If you set `max_pairs=1000`, then any key that generates more than 1000
            pairs will be ignored.
        task:
            The task to count pairs for.

            - "link": each key results in n_left * n_right pairs
            - "dedupe": each key results in n_left * (n_right - 1) / 2 pairs
               since we will only generate pair (A, B), not also (B, A).
            - None: inferred from the input tables: if `left is right`, then "dedupe",
              otherwise "link".
        """  # noqa: E501
        # TODO: support named keys, eg KeyLinker("age", city=_.city.upper())
        self.resolvers = tuple(_resolve.key_pair_resolvers(keys))
        self.max_pairs = max_pairs
        self.task = task

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        keys_left, keys_right = self.__keys__(left, right)
        clauses = (kl == kr for kl, kr in zip(keys_left, keys_right, strict=True))
        if self.max_pairs is not None:
            too_common_left, too_common_right = self.too_common_of_records(left, right)
            clauses = [
                *clauses,
                left.record_id.notin(too_common_left.record_id),
                # right.record_id.notin(too_common_right.record_id),
            ]
        return ibis.and_(*clauses)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        """The linkage between the two tables."""
        # Is this the right place to be calling .view()?
        task = infer_task(task=self.task, left=left, right=right)
        if right is left:
            right = right.view()
        condition = self.__join_condition__(left, right)
        if task == "dedupe":
            condition = condition & (left.record_id < right.record_id)
        return Linkage.from_join_condition(left=left, right=right, condition=condition)

    def too_common_of_records(
        self, left: ibis.Table, right: ibis.Table
    ) -> tuple[ibis.Table, ibis.Table]:
        """The prefilter clause that removes keys that would generate too many pairs."""
        if self.max_pairs is None:
            return left.limit(0), right.limit(0)
        # TODO: this should really use self.pair_counts()
        left_keys, right_keys = self.__keys__(left, right)
        left_key = ibis.struct({k.get_name(): k for k in left_keys}).hash()
        right_key = ibis.struct({k.get_name(): k for k in right_keys}).hash()
        key_name = _util.unique_name()
        # count_name = _util.unique_name()
        left = left.mutate(left_key.name(key_name))
        right = right.mutate(right_key.name(key_name))
        left_counts = left.group_by(key_name).agg(nleft=_.count())
        right_counts = right.group_by(key_name).agg(nright=_.count())
        # If a key is only in one table, it won't be in this joined table,
        # so watch out, we should only test for `IN`, not `NOT IN`
        pair_counts_by_key = ibis.join(
            left_counts,
            right_counts,
            key_name,
        ).select(key_name, npairs=_.nleft * _.nright)
        too_big = pair_counts_by_key.filter(_.npairs > self.max_pairs)
        too_big = too_big.cache()
        left = left.filter(_[key_name].isin(too_big[key_name])).drop(key_name)
        right = right.filter(_[key_name].isin(too_big[key_name])).drop(key_name)
        return left, right

    def __keys__(
        self, left: ibis.Table, right: ibis.Table
    ) -> list[tuple[ir.Column, ir.Column]]:
        lkeys = []
        rkeys = []
        for resolver_l, resolver_r in self.resolvers:
            lkeys.append(resolver_l(left))
            rkeys.append(resolver_r(right))
        return tuple(lkeys), tuple(rkeys)

    def key_counts_left(self, left: ibis.Table, /) -> KeyCountsTable:
        keys = [resolver(left) for resolver, _ in self.resolvers]
        return key_counts(left.select(*keys))

    def key_counts_right(self, right: ibis.Table, /) -> KeyCountsTable:
        keys = [resolver(right) for _, resolver in self.resolvers]
        return key_counts(right.select(*keys))

    def pair_counts(
        self,
        left: ibis.Table,
        right: ibis.Table,
    ) -> PairCountsTable:
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
        ...     (1, "a", 1),
        ...     (2, "b", 1),
        ...     (3, "b", 1),
        ...     (4, "c", 3),
        ...     (5, "b", 2),
        ...     (6, "c", 3),
        ...     (7, None, 4),
        ...     (8, "c", 3),
        ... ]
        >>> t = ibis.memtable(
        ...     records, schema={"record_id": int, "letter": str, "num": int}
        ... ).cache()
        >>> t
        ┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┓
        ┃ record_id ┃ letter ┃ num   ┃
        ┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━┩
        │ int64     │ string │ int64 │
        ├───────────┼────────┼───────┤
        │         1 │ a      │     1 │
        │         2 │ b      │     1 │
        │         3 │ b      │     1 │
        │         4 │ c      │     3 │
        │         5 │ b      │     2 │
        │         6 │ c      │     3 │
        │         7 │ NULL   │     4 │
        │         8 │ c      │     3 │
        └───────────┴────────┴───────┘

        If we joined t with itself using this blocker in a dedupe task,
        we would end up with

        - 3 pairs in the (c, 3) block due to pairs (3, 6), (3, 8), and (6, 8)
        - 1 pairs in the (b, 1) block due to pairs (1, 2)
        - 0 pairs in the (a, 1) block due to record 0 not getting blocked with itself
        - 0 pairs in the (b, 2) block due to record 4 not getting blocked with itself

        >>> linker = mismo.KeyLinker(["letter", "num"], task="dedupe")
        >>> counts = linker.pair_counts(t, t)
        >>> counts.order_by("letter", "num")
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

        If we joined t with a copy of itself using this linker in a link task,
        we would end up with

        - 9 pairs in the (c, 3) block due to pairs (3,3), (3, 6), (3, 8), (6, 3), (6, 6), (6, 8), (8, 3), (8, 6), and (8, 8)
        - 4 pairs in the (b, 1) block due to pairs (1, 1), (1, 2), (2, 1), and (2, 2)
        - 1 pairs in the (a, 1) block due to pair (0, 0)
        - 1 pairs in the (b, 2) block due to pair (4, 4)

        >>> linker = mismo.KeyLinker(["letter", "num"], task="link")
        >>> counts = linker.pair_counts(t, t)
        >>> counts.order_by("letter", "num")
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

        The returned CountsTable is a subclass of an Ibis Table
        with a special `n_total` method for convenience:

        >>> counts.n_total()
        15
        >>> isinstance(counts, ibis.Table)
        True
        """  # noqa: E501
        too_common_left, too_common_right = self.too_common_of_records(left, right)
        left_filtered = left.filter(_.record_id.notin(too_common_left.record_id))
        right_filtered = right.filter(_.record_id.notin(too_common_right.record_id))
        counts = pair_counts(
            self.resolvers, left_filtered, right_filtered, task=self.task
        )
        if self.max_pairs is not None:
            counts = PairCountsTable(counts.filter(_.n <= self.max_pairs))
        return counts

    def __repr__(self) -> str:
        if self.max_pairs is None:
            max_pairs = "None"
        else:
            max_pairs = f"{self.max_pairs:_}"
        resolvers_str = ", ".join(str(r) for r in self.resolvers)
        return f"{self.__class__.__name__}([{resolvers_str}], max_pairs={max_pairs})"


class KeyLinksTable(LinksTable):
    def __init__(
        self, links: ibis.Table, *, left: ibis.Table, right: ibis.Table, keys, task
    ):
        super().__init__(links, left=left, right=right)
        object.__setattr__(self, "_keys", keys)
        object.__setattr__(self, "_task", task)

    def count(self) -> ibis.ir.IntegerScalar:
        """The number of pairs in the links table."""
        return (
            pair_counts(
                left=self.left_,
                right=self.right_,
                key_pairs=self._keys,
                task=self._task,
            )
            .n.sum()
            .fill_null(0)
            .name("count")
        )


def key_counts(
    key_or_table: ibis.Table | ibis.Value | ibis.Deferred,
    *keys: ibis.Value | ibis.Deferred | Any,
) -> KeyCountsTable:
    t = _util.select(key_or_table, *keys)
    t = t.drop_null(how="any")
    t = t.group_by(t.columns).agg(n=_.count()).order_by(_.n.desc())
    return KeyCountsTable(t)


def pair_counts(
    key_pairs,
    left: ibis.Table,
    right: ibis.Table,
    *,
    task: Literal["dedupe", "link"] | None = None,
) -> PairCountsTable:
    resolvers = _resolve.key_pair_resolvers(key_pairs)
    keys_left = [resolver(left) for resolver, _ in resolvers]
    keys_right = [resolver(right) for _, resolver in resolvers]
    kcl = key_counts(left.select(*keys_left))
    kcr = key_counts(right.select(*keys_right))
    task = infer_task(task=task, left=left, right=right)
    if task == "dedupe":
        by_key = kcl.mutate(n=(_.n * (_.n - 1) / 2).cast(int))
    else:
        if kcl.equals(kcr):
            kcr = kcr.view()
        condition = ibis.and_(
            *(
                kcl[a] == kcr[b]
                for a, b in zip(kcl.columns, kcr.columns, strict=True)
                if a != "n" and b != "n"
            )
        )
        by_key = (
            ibis.join(kcl, kcr, condition).mutate(n=_.n * _.n_right).drop("n_right")
        )
    by_key = by_key.order_by(_.n.desc())
    return PairCountsTable(by_key)
