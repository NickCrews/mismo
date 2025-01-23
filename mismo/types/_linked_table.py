from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

import ibis
from ibis import _

from mismo import _typing, _util
from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    import altair as alt
    from ibis.expr import types as ir


class LinkedTable(TableWrapper):
    """A table of records that are linked to another table.

    This acts just like an Ibis Table, but it has a few extra attributes
    and methods that make it more ergonomic to work with,
    eg to add data from the linked table.
    """

    other_: ibis.Table
    """The other table that this table is linked to.
    Trailing underscore to avoid name conflicts with column names."""
    links_: ibis.Table
    """The table of links between this table and `other`.
    Trailing underscore to avoid name conflicts with column names."""

    # Not public, just putting them here for IDE type hints
    _self_id: str
    _other_id: str

    def __init__(self, table: ibis.Table, other: ibis.Table, links: ibis.Table):
        if len(links.columns) != 2:
            raise ValueError("links must have exactly two columns")
        self_id, other_id = links.columns
        if self_id not in table.columns:
            raise ValueError(f"{self_id} not in table")
        if other_id not in other.columns:
            raise ValueError(f"{other_id} not in other")

        super().__init__(table)
        object.__setattr__(self, "other_", other)
        object.__setattr__(self, "links_", links)
        object.__setattr__(self, "_other_id", other_id)
        object.__setattr__(self, "_self_id", self_id)

    def with_many_linked_values(
        self,
        *values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
        **named_values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
    ) -> _typing.Self:
        """
        This table, with `array<>` columns of values from linked records in `other`

        This is very similar to `with_single_linked_values`, except:

        - This includes values from all linked records, not just the single match.
        - Here, the values from the N linked records are returned in an array.
          There, since there is only one linked record, we return it directly,
          not as a length-1 array.

        This uses the same semantics as `ibis.Table.select(*values, **named_values)`
        to choose which values from `other` to add to `self`

        If no values are provided, we will by default add a column named `other`
        with all the values from other packed into a struct.

        Parameters
        ----------
        values
            unnamed values
        named_values
            named values

        Returns
        -------
        A new LinkedTable with the new column.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> this = ibis.memtable({"idl": [4, 5, 6]})
        >>> other = ibis.memtable({"idr": [7, 8, 9]})
        >>> links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
        >>> lt = LinkedTable(this, other, links)
        >>> lt
        ┏━━━━━━━┓
        ┃ idl   ┃
        ┡━━━━━━━┩
        │ int64 │
        ├───────┤
        │     4 │
        │     5 │
        │     6 │
        └───────┘

        Get the "idr" values from all the linked records,
        as well as create a derived value "plus_one" that is "idr" + 1:

        >>> lt.with_many_linked_values("idr", plus_one=_.idr + 1)  # doctest: +SKIP
        ┏━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
        ┃ idl   ┃ idr          ┃ plus_one     ┃
        ┡━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
        │ int64 │ array<int64> │ array<int64> │
        ├───────┼──────────────┼──────────────┤
        │     4 │ [7, 8]       │ [8, 9]       │
        │     5 │ [9]          │ [10]         │
        │     6 │ []           │ []           │
        └───────┴──────────────┴──────────────┘

        Default is to pack everything into array<struct<all columns from other>>:

        >>> lt.with_many_linked_values()
        ┏━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ idl   ┃ other                     ┃
        ┡━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64 │ array<struct<idr: int64>> │
        ├───────┼───────────────────────────┤
        │     4 │ [{'idr': 7}, {'idr': 8}]  │
        │     5 │ [{'idr': 9}]              │
        │     6 │ []                        │
        └───────┴───────────────────────────┘
        """
        if not values and not named_values:
            values = (
                lambda t: ibis.struct({c: t[c] for c in t.columns}).name("other"),
            )

        uname = _util.unique_name()
        o = self.other_.select(_[self._other_id].name(uname), *values, **named_values)
        self_id_to_other_vals = (
            self.links_.rename(**{uname: self._other_id}).join(o, uname).drop(uname)
        )
        value_names = [c for c in self_id_to_other_vals.columns if c != self._self_id]
        self_id_to_other_array_vals = self_id_to_other_vals.group_by(self._self_id).agg(
            *[_[c].collect().name(c) for c in value_names]
        )

        t = self
        need_to_drop = [c for c in value_names if c in t.columns]
        t = t.drop(*need_to_drop)
        with_other = _util.join_lookup(
            self,
            self_id_to_other_array_vals,
            self._self_id,
            defaults={c: [] for c in value_names},
        )
        return self.__class__(with_other, self.other_, self.links_)

    def with_single_linked_values(
        self,
        *values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
        **named_values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
    ) -> _typing.Self:
        """
        This table filtered to single matches, with values from the linked record.

        This is very similar to `with_many_linked_values`, except:

        - It filters to only include records that have exactly 1 link.
        - In `with_many_linked_values` the values from the N linked records are
          returned in an array.
          Here, since there is only one linked record, we return it directly,
          not as a length-1 array.

        This uses the same semantics as `ibis.Table.select(*values, **named_values)`
        to choose which values from `other` to add to `self`

        If no values are provided, we will by default add a column named `other`
        with all the values from other packed into a struct.

        Parameters
        ----------
        values
            unnamed values
        named_values
            named values

        Returns
        -------
        A new LinkedTable with the new column.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> this = ibis.memtable({"idl": [4, 5, 6]})
        >>> other = ibis.memtable({"idr": [7, 8, 9]})
        >>> links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
        >>> lt = LinkedTable(this, other, links)
        >>> lt
        ┏━━━━━━━┓
        ┃ idl   ┃
        ┡━━━━━━━┩
        │ int64 │
        ├───────┤
        │     4 │
        │     5 │
        │     6 │
        └───────┘

        Get the "idr" value from the linked record,
        as well as create a derived value "plus_one" that is "idr" + 1:

        >>> lt.with_single_linked_values("idr", plus_one=_.idr + 1)
        ┏━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
        ┃ idl   ┃ idr   ┃ plus_one ┃
        ┡━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
        │ int64 │ int64 │ int64    │
        ├───────┼───────┼──────────┤
        │     5 │     9 │       10 │
        └───────┴───────┴──────────┘

        Default is to pack everything into a struct:

        >>> lt.with_single_linked_values()  # doctest: +SKIP
        ┏━━━━━━━┳━━━━━━━━━━━━━━━━━━━━┓
        ┃ idl   ┃ other              ┃
        ┡━━━━━━━╇━━━━━━━━━━━━━━━━━━━━┩
        │ int64 │ struct<idr: int64> │
        ├───────┼────────────────────┤
        │     5 │ {'idr': 9}         │
        └───────┴────────────────────┘
        """  # noqa: E501
        if not values and not named_values:
            values = (
                lambda t: ibis.struct({c: t[c] for c in t.columns}).name("other"),
            )

        uname = _util.unique_name()
        t = self.with_n_links(uname).filter(_[uname] == 1).drop(uname)
        o = self.other_.select(_[self._other_id].name(uname), *values, **named_values)
        self_id_to_other_vals = (
            self.links_.rename(**{uname: self._other_id}).join(o, uname).drop(uname)
        )

        need_to_drop = [
            c
            for c in self_id_to_other_vals.columns
            if c in t.columns and c != self._self_id
        ]
        t = t.drop(*need_to_drop)
        with_other = _util.join_lookup(t, self_id_to_other_vals, self._self_id)
        return self.__class__(with_other, self.other_, self.links_)

    @property
    def _n_links_by_id(self) -> ir.Table:
        return _n_links_by_id(self.select(self._self_id), self.links_)

    def with_n_links(self, name: str = "n_links") -> _typing.Self:
        """
        Add a column to this table with the number of links each record has.

        Parameters
        ----------
        name
            The name of the new column.

        Returns
        -------
        A new LinkedTable with the new column.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"idl": [4, 5, 6]})
        >>> right = ibis.memtable({"idr": [7, 8, 9]})
        >>> links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
        >>> linkage = Linkage(left, right, links)
        >>> linkage.left.with_n_links().order_by("idl")
        ┏━━━━━━━┳━━━━━━━━━┓
        ┃ idl   ┃ n_links ┃
        ┡━━━━━━━╇━━━━━━━━━┩
        │ int64 │ int64   │
        ├───────┼─────────┤
        │     4 │       2 │
        │     5 │       1 │
        │     6 │       0 │
        └───────┴─────────┘
        >>> linkage.right.with_n_links().order_by("idr")
        ┏━━━━━━━┳━━━━━━━━━┓
        ┃ idr   ┃ n_links ┃
        ┡━━━━━━━╇━━━━━━━━━┩
        │ int64 │ int64   │
        ├───────┼─────────┤
        │     7 │       1 │
        │     8 │       1 │
        │     9 │       1 │
        └───────┴─────────┘
        """
        n_by_id = self._n_links_by_id.rename(**{name: "n_links"})
        t = self
        if name in t.columns:
            t = t.drop(name)
        added = _util.join_lookup(t, n_by_id, self._self_id)
        return self.__class__(added, self.other_, self.links_)

    def link_counts(self) -> LinkCountsTable:
        """
        Describes 'There are `n_records` in self that linked to `n_links` in `other'.

        This is basically a histogram of `self.with_n_links()`

        See Also
        --------
        with_n_links

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"idl": [4, 5, 6]})
        >>> right = ibis.memtable({"idr": [7, 8, 9]})
        >>> links = ibis.memtable({"idl": [4, 4, 5], "idr": [7, 8, 9]})
        >>> linkage = Linkage(left, right, links)

        There is 1 record in left (6) that didn't match any in right.
        There is 1 record in left (5) that matched 1 in right.
        There is 1 record in left (4) that matched 2 in right.

        >>> linkage.left.link_counts()
        ┏━━━━━━━━━━━┳━━━━━━━━━┓
        ┃ n_records ┃ n_links ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━┩
        │ int64     │ int64   │
        ├───────────┼─────────┤
        │         1 │       2 │
        │         1 │       1 │
        │         1 │       0 │
        └───────────┴─────────┘

        All 3 records in right matched 1 in left.

        >>> linkage.right.link_counts()
        ┏━━━━━━━━━━━┳━━━━━━━━━┓
        ┃ n_records ┃ n_links ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━┩
        │ int64     │ int64   │
        ├───────────┼─────────┤
        │         3 │       1 │
        └───────────┴─────────┘
        """
        counts = (
            self._n_links_by_id.n_links.value_counts(name="n_records")
            .order_by(_.n_links.desc())
            .select("n_records", "n_links")
        )
        return LinkCountsTable(counts)


class Linkage:
    """Two tables of records and links between them."""

    def __init__(self, left: ibis.Table, right: ibis.Table, links: ibis.Table):
        if len(links.columns) != 2:
            raise ValueError("links must have exactly two columns")
        left_id = links.columns[0]
        right_id = links.columns[1]

        if left_id not in left.columns:
            raise ValueError(f"{left_id} not in left")
        if right_id not in right.columns:
            raise ValueError(f"{right_id} not in right")

        self._left = LinkedTable(left, right, links.select(left_id, right_id))
        self._right = LinkedTable(right, left, links.select(right_id, left_id))
        self._links = links
        self._left_id = left_id
        self._right_id = right_id

    @property
    def left(self) -> LinkedTable:
        """The left Table."""
        return self._left

    @property
    def right(self) -> LinkedTable:
        """The right Table."""
        return self._right

    @property
    def links(self) -> ibis.Table:
        """A table of (<left id>, <right id>) pairs that link `left` and `right`."""
        return self._links

    @property
    def left_id(self) -> str:
        """The column that serves as a record ID in `left`."""
        return self._left_id

    @property
    def right_id(self) -> str:
        """The column that serves as a record ID in `right`."""
        return self._right_id

    @classmethod
    def from_predicates(
        cls,
        left: ibis.Table,
        right: ibis.Table,
        predicates,
        *,
        left_id: str | None = None,
        right_id: str | None = None,
    ) -> _typing.Self:
        """
        Create a Linkage from join predicates.

        This is useful if you don't already have a table of links.
        This will create a table of links by joining the left and right tables
        on the given predicates.
        It will either use the existing id columns in the tables or create new unique ones.

        Parameters
        ----------
        left
            The left table.
        right
            The right table.
        predicates
            The join predicates. Anything that ibis.join() accepts.
        left_id
            The name of the id column in the left table.
            If None, a new unique name will be generated.
            If this column is present in the left table, nothing will be done.
            If it's not present, a new ID column will be created.
        right_id
            Same as left_id, but for the right table.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> tl = ibis.memtable({"x": [1, 2, 3]})
        >>> tr = ibis.memtable({"x": [1, 2, 2]})
        >>> linkage = Linkage.from_predicates(tl, tr, "x")

        We added an id column automatically:

        >>> linkage.left  # doctest: +SKIP
        ┏━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ x     ┃ id_l_6K9WXYNTN0ZK0OXFCLR3M3Q6S ┃
        ┡━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64 │ int64                          │
        ├───────┼────────────────────────────────┤
        │     1 │                              0 │
        │     2 │                              1 │
        │     3 │                              2 │
        └───────┴────────────────────────────────┘

        and it's in the links table:

        >>> linkage.links  # doctest: +SKIP
        ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ id_l_6K9WXYNTN0ZK0OXFCLR3M3Q6S ┃ id_r_77IJWQ0DV0QRAF5JCQIB5VAIE ┃
        ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64                          │ int64                          │
        ├────────────────────────────────┼────────────────────────────────┤
        │                              0 │                              0 │
        │                              1 │                              2 │
        │                              1 │                              1 │
        └────────────────────────────────┴────────────────────────────────┘

        We can pass the name we want:

        >>> Linkage.from_predicates(tl, tr, "x", left_id="my_id_l").left
        ┏━━━━━━━┳━━━━━━━━━┓
        ┃ x     ┃ my_id_l ┃
        ┡━━━━━━━╇━━━━━━━━━┩
        │ int64 │ int64   │
        ├───────┼─────────┤
        │     1 │       0 │
        │     2 │       1 │
        │     3 │       2 │
        └───────┴─────────┘
        """  # noqa: E501
        if left_id is None:
            left_id = _util.unique_name("id_l")
        if right_id is None:
            right_id = _util.unique_name("id_r")
        if left_id not in left.columns:
            left = left.mutate(ibis.row_number().name(left_id))
        if right_id not in right.columns:
            right = right.mutate(ibis.row_number().name(right_id))
        links = ibis.join(left, right, predicates).select(left_id, right_id)
        return cls(left, right, links)

    def link_counts_chart(self) -> alt.Chart:
        """
        A side by side altair Chart of `left.link_counts(`)` and `right.link_counts()`
        """
        import altair as alt

        left = self.left.link_counts().chart()
        right = self.right.link_counts().chart().properties(title="Right Table")
        subtitle = left.title.subtitle
        left = left.properties(title=alt.TitleParams("Left Table", anchor="middle"))
        right = right.properties(title=alt.TitleParams("Right Table", anchor="middle"))
        return alt.hconcat(left, right).properties(
            title=alt.TitleParams(
                "Number of Records by Link Count", subtitle=subtitle, anchor="middle"
            )
        )

    def to_parquets(self, directory: str | Path, /) -> None:
        """
        Write the needle, haystack, and links to parquet files in the given directory.
        """
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        self.left.to_parquet(d / "left.parquet")
        self.right.to_parquet(d / "right.parquet")
        self.links.to_parquet(d / "links.parquet")

    @classmethod
    def from_parquets(
        cls, directory: str | Path, /, *, backend: ibis.BaseBackend | None = None
    ) -> _typing.Self:
        """Create a FindResults by reading parquets from the given directory."""
        if backend is None:
            backend = ibis
        d = Path(directory)
        return cls(
            left=backend.read_parquet(d / "left.parquet"),
            right=backend.read_parquet(d / "right.parquet"),
            links=backend.read_parquet(d / "links.parquet"),
        )


class LinkCountsTable(TableWrapper):
    """A table representing the number of records binned by number of links.

    eg "There were 700 records with 0 links, 300 with 1 link, 20 with 2 links, ..."
    """

    n_records: ir.IntegerColumn
    """The number of records."""
    n_links: ir.IntegerColumn
    """The number of links."""

    def __init__(self, t):
        if set(t.columns) != {"n_records", "n_links"}:
            raise ValueError(
                "LinkCountsTable must have exactly columns 'n_records' and 'n_links'"
            )
        super().__init__(t)

    def chart(self) -> alt.Chart:
        """A bar chart of the number of records by the number of links."""
        import altair as alt

        n_title = "Number of Records"
        key_title = "Number of Links"
        mins = self.order_by(_.n_links.asc()).limit(2).execute()
        if len(mins) > 1:
            subtitle = f"eg '{mins.n_records[0]:,} records had {mins.n_links[0]} links, {mins.n_records[1]:,} had {mins.n_links[1]} links, ...'"  # noqa: E501
        elif len(mins) == 1:
            subtitle = (
                f"eg '{mins.n_records[0]:,} records had {mins.n_links[0]} links', ..."  # noqa: E501
            )
        else:
            subtitle = "eg 'there were 1000 records with 0 links, 500 with 1 link, 100 with 2 links, ...'"  # noqa: E501
        chart = (
            alt.Chart(self)
            .properties(
                title=alt.TitleParams(
                    "Number of Records by Link Count",
                    subtitle=subtitle,
                    anchor="middle",
                ),
                width=alt.Step(12) if self.count().execute() <= 20 else alt.Step(8),
            )
            .mark_bar()
            .encode(
                # if we ever change this sorting, keep the subtitle example
                # to be in sync of the same order as the bars go left to right.
                alt.X("n_links:O", title=key_title, sort="x"),
                alt.Y(
                    "n_records:Q",
                    title=n_title,
                    scale=alt.Scale(type="symlog"),
                ),
                tooltip=[
                    alt.Tooltip("n_records:Q", title=n_title, format=","),
                    alt.Tooltip("n_links:O", title=key_title),
                ],
            )
        )
        return chart


def _n_links_by_id(ids: ir.Table, links: ir.Table) -> ibis.Table:
    if len(ids.columns) != 1:
        raise ValueError("ids must have exactly one column")
    id_col = ids.columns[0]

    if len(links.columns) != 2:
        raise ValueError("links must have exactly two columns")
    if id_col not in links.columns:
        raise ValueError(f"{id_col} not in links")
    other_id = links.columns[0] if links.columns[1] == id_col else links.columns[1]

    n_links_by_id = links.group_by(id_col).aggregate(n_links=_[other_id].nunique())
    # The above misses records with no entries in the links table (eg unlinked records)
    records_not_linked = (
        ids.distinct().filter(_[id_col].notin(links[id_col])).mutate(n_links=0)
    )
    # the default of 0 is an int8, which isn't unionable with the int64 of other table
    records_not_linked = records_not_linked.cast(n_links_by_id.schema())
    n_links_by_id = ibis.union(n_links_by_id, records_not_linked)
    return n_links_by_id


def _resolve(t: ibis.Table, resolver) -> ir.Value:
    if isinstance(resolver, ibis.Deferred):
        return resolver.resolve(t)
    elif isinstance(resolver, str):
        return t[resolver]
    else:
        return resolver(t)
