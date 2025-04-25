from __future__ import annotations

from typing import Callable, Iterable, Literal

import ibis
from ibis import Deferred, _
from ibis.expr import types as ir

from mismo import _funcs, _typing, _util, joins
from mismo._counts_table import KeyCountsTable, PairCountsTable
from mismo.linkage._linkage import BaseLinkage, LinkTableLinkage
from mismo.linkage._linker import Linker, infer_task
from mismo.types import LinkedTable, LinksTable


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
        # TODO: support named keys, eg KeyLinker("age", city=_.city.upper())
        self.keys = tuple(_util.promote_list(keys))
        self.max_pairs = max_pairs

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ir.BooleanValue:
        keys_left, keys_right = self.__keys__(left, right)
        clauses = (kl == kr for kl, kr in zip(keys_left, keys_right, strict=True))
        return ibis.and_(*clauses)

    def __pre_join__(
        self, left: ibis.Table, right: ibis.Table
    ) -> tuple[ibis.Table, ibis.Table]:
        """The prefilter clause that removes keys that would generate too many pairs."""
        if self.max_pairs is None:
            return left, right
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
        too_big = pair_counts_by_key.filter(_.npairs > self.max_pairs)[key_name]
        left = left.filter(_[key_name].notin(too_big)).drop(key_name)
        right = right.filter(_[key_name].notin(too_big)).drop(key_name)
        return left, right

    def __links__(self, left: ibis.Table, right: ibis.Table) -> LinksTable:
        """The links between the two tables."""
        left, right = self.__pre_join__(left, right)
        links = joins.join(
            left,
            right,
            self.__join_condition__,
            lname="{name}_l",
            rname="{name}_r",
            rename_all=True,
        )
        return LinksTable(links, left=left, right=right)

    def linkage(self, left: ibis.Table, right: ibis.Table) -> KeyLinkage:
        """The linkage between the two tables."""
        return KeyLinkage(
            left=left, right=right, keys=self.keys, max_pairs=self.max_pairs
        )

    def __keys__(
        self, left: ibis.Table, right: ibis.Table
    ) -> list[tuple[ir.Column, ir.Column]]:
        return _get_keys_2_tables(left, right, self.keys)

    def __link__(self, left: ibis.Table, right: ibis.Table) -> KeyLinkage:
        return KeyLinkage(left=left, right=right, keys=self.keys, task=self.task)

    def key_counts(self, t: ibis.Table, /) -> KeyCountsTable:
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
        >>> linker = mismo.KeyLinker(["letter", "num"])

        Note how the (None, 4) record is not counted,
        since NULLs are not counted as a match during a join.

        >>> counts = linker.key_counts(t)
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
        return key_counts(self.keys, t)

    def pair_counts(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
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
        >>> linker = mismo.KeyLinker(["letter", "num"])

        If we joined t with itself using this blocker in a link task,
        we would end up with

        - 9 pairs in the (c, 3) block due to pairs (3,3), (3, 6), (3, 8), (6, 3), (6, 6), (6, 8), (8, 3), (8, 6), and (8, 8)
        - 4 pairs in the (b, 1) block due to pairs (1, 1), (1, 2), (2, 1), and (2, 2)
        - 1 pairs in the (a, 1) block due to pair (0, 0)
        - 1 pairs in the (b, 2) block due to pair (4, 4)

        >>> counts = linker.pair_counts(t, t, task="link").order_by("letter", "num")
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

        The returned CountsTable is a subclass of an Ibis Table
        with a special `n_total` property for convenience:

        >>> counts.n_total
        4
        >>> isinstance(counts, ibis.Table)
        True
        """  # noqa: E501
        return pair_counts(self.keys, left, right, task=task)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.keys!r}, max_pairs={self.max_pairs:_})"  # noqa: E501


class KeyLinkage(BaseLinkage):
    """
    A [Linkage][mismo.Linkage] of where records share a key, eg

    This is useful if you don't already have a table of links.
    This will create a table of links by joining the left and right tables
    on the given predicates.
    It will either use the existing `record_id` columns in the tables,
    or create new ones if they don't exist.

    Parameters
    ----------
    left
        The left table.
    right
        The right table.
    predicates
        The join predicates. Anything that ibis.join() accepts.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> tl = ibis.memtable({"record_id": [1, 2, 3]})
    >>> tr = ibis.memtable({"record_id": [1, 2, 2]})
    >>> linkage = KeyLinkage(tl, tr, ["record_id"])
    >>> linkage.links
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
    │ int64       │ int64       │
    ├─────────────┼─────────────┤
    │           1 │           1 │
    │           2 │           2 │
    │           2 │           2 │
    └─────────────┴─────────────┘
    """

    def __init__(
        self,
        left: ibis.Table,
        right: ibis.Table,
        keys,
        *,
        max_pairs: int | None = None,
        task: Literal["dedupe", "link"] | None = None,
    ) -> None:
        # if isinstance(keys, tuple) and len(keys) == 2:
        #     keys = [keys]
        task = infer_task(task, left, right)
        if task == "dedupe":
            right = right.view()
        self.task = task
        self.keys = keys
        self.max_pairs = max_pairs
        self._linker = KeyLinker(keys, max_pairs=max_pairs)
        self._links = self._linker.__links__(left, right)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return joins.MultiKeyJoinCondition(self.keys).__join_condition__(left, right)

    @property
    def left(self):
        return self._links.left_

    @property
    def right(self):
        return self._links.right_

    @property
    def links(self):
        return KeyLinksTable(
            self._links,
            left=self.left,
            right=self.right,
            keys=self.keys,
            task=self.task,
        )

    def adjust(
        self,
        *,
        left: LinkedTable | None = None,
        right: LinkedTable | None = None,
        links: LinksTable | None = None,
    ) -> LinkTableLinkage:
        return LinkTableLinkage(
            left=left if left is not None else self.left,
            right=right if right is not None else self.right,
            links=links if links is not None else self.links,
        )

    def cache(self) -> _typing.Self:
        """
        No-op to cache this, since the condition is an abstract condition, and
        left and right are already cached.
        """
        # I think this API is fine to just return self instead of a new instance?
        return self

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<keys={_util.get_name(self.keys)}, nleft={self.left.count().execute():_}, nright={self.right.count().execute():_}, nlinks={self.links.count().execute():_}>"  # noqa: E501


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
                keys=self._keys,
                task=self._task,
            )
            .n.sum()
            .fill_null(0)
        )


def _get_keys_2_tables(
    t1: ibis.Table, t2: ibis.Table, keys
) -> tuple[tuple[ir.Value], tuple[ir.Value]]:
    if isinstance(keys, ibis.Value):
        return (keys,), (keys,)
    if isinstance(keys, ibis.Deferred):
        return t1.bind(keys), t2.bind(keys)
    if isinstance(keys, str):
        return t1.bind(keys), t2.bind(keys)
    if _funcs.is_unary(keys):
        return t1.bind(keys), t2.bind(keys)
    if _funcs.is_binary(keys):
        return _get_keys_2_tables(keys(t1, t2))
    # TODO: this might not be complete, needs tests
    return t1.bind(keys), t2.bind(keys)


def key_counts(keys: tuple, t: ibis.Table) -> KeyCountsTable:
    t = t.select(keys)
    t = t.drop_null(how="any")
    t = t.group_by(t.columns).agg(n=_.count()).order_by(_.n.desc())
    return KeyCountsTable(t)


def pair_counts(
    keys,
    left: ibis.Table,
    right: ibis.Table,
    *,
    task: Literal["dedupe", "link"] | None = None,
) -> PairCountsTable:
    task = infer_task(task, left, right)
    kcl = key_counts(keys, left)
    kcr = key_counts(keys, right)
    if task == "dedupe":
        by_key = kcl.mutate(n=(_.n * (_.n - 1) / 2).cast(int))
    else:
        k = [c for c in kcl.columns if c != "n"]
        by_key = ibis.join(kcl, kcr, k).mutate(n=_.n * _.n_right).drop("n_right")
    by_key = by_key.order_by(_.n.desc())
    return PairCountsTable(by_key)
