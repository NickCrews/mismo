from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _common, _typing, _util
from mismo.types._links_table import LinksTable
from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    import altair as alt


class LinkedTable(TableWrapper):
    """A table of records that are linked to another table.

    Each record here can be linked to 0-N records in the `other_`.

    This acts just like an Ibis Table, but it has a few extra attributes
    and methods that make it more ergonomic to work with,
    eg to add data from the linked table.
    """

    def __init__(self, table: ibis.Table, *, other: ibis.Table, links: ibis.Table):
        _common.check_tables_and_links(table, other, links)

        super().__init__(table)
        object.__setattr__(self, "_other_raw", other)
        object.__setattr__(self, "_links_raw", links)

    @property
    def other_(self) -> _typing.Self:
        """The other table that this table is linked to.

        Trailing underscore to avoid name conflicts with column names."""
        # Need to make sure that we swap the _l and _r suffixes in the links table
        return self.__class__(
            self._other_raw,
            other=self,
            links=LinksTable._swap_perspective(self._links_raw),
        )

    @property
    def links_(self) -> LinksTable:
        """The table of links between this table and `other`.

        Trailing underscore to avoid name conflicts with column names."""
        return LinksTable(self._links_raw, left=self, right=self._other_raw)

    def with_linked_values(
        self,
        *values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
        default_agg: ibis.Deferred = _.collect(),
        **named_values: ibis.Deferred | Callable[[ibis.Table], ir.Value] | None,
    ) -> _typing.Self:
        """
        This table, with values from the linked records.

        See the examples below, this is easy to understand with examples,
        but hard to describe.

        Parameters
        ----------
        values
            unnamed values
        named_values
            named values

        Returns
        -------
        A new LinkedTable with new columns of values from the linked records.

        Examples
        --------
        >>> import ibis
        >>> ibis.options.interactive = True
        >>> this = ibis.memtable({"record_id": [40, 50, 60], "x": ["a", "b", "c"]})
        >>> other = ibis.memtable({"record_id": [41, 42, 51], "y": [4.1, 4.2, 9.0]})
        >>> links = ibis.memtable(
        ...     {"record_id_l": [40, 40, 50], "record_id_r": [41, 42, 51]}
        ... )
        >>> lt = LinkedTable(this, other=other, links=links)
        >>> lt
        LinkedTable<3 records, 3 links>
        ┏━━━━━━━━━━━┳━━━━━━━━┓
        ┃ record_id ┃ x      ┃
        ┡━━━━━━━━━━━╇━━━━━━━━┩
        │ int64     │ string │
        ├───────────┼────────┤
        │        40 │ a      │
        │        50 │ b      │
        │        60 │ c      │
        └───────────┴────────┘

        Select exactly which values you want from `other`
        Since every record in `self` can be linked to 0-N records in `other`,
        we need a way of aggregating the 0-N linked values in `other` to a single value.
        By default, if the given expression is not an aggregate,
        we default to `.collect()` to combine them all into an array.
        If the given expression is already an aggregate, then it is kept as-is.

        This uses the same semantics as `ibis.Table.select(*values, **named_values)`.

        >>> lt.with_linked_values(
        ...     "y",
        ...     ibis._.y.max().name("y_max"),
        ...     others="record_id",
        ...     other=ibis._.record_id.arbitrary(),
        ... )
        LinkedTable<3 records, 3 links>
        ┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
        ┃ record_id ┃ x      ┃ y                    ┃ y_max   ┃ others               ┃ other ┃
        ┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━┩
        │ int64     │ string │ array<float64>       │ float64 │ array<int64>         │ int64 │
        ├───────────┼────────┼──────────────────────┼─────────┼──────────────────────┼───────┤
        │        40 │ a      │ [4.1, 4.2]           │     4.2 │ [41, 42]             │    41 │
        │        50 │ b      │ [9.0]                │     9.0 │ [51]                 │    51 │
        │        60 │ c      │ NULL                 │    NULL │ NULL                 │  NULL │
        └───────────┴────────┴──────────────────────┴─────────┴──────────────────────┴───────┘

        One common use case for this is during the task of a lookup.
        I want to see the record id of the linked record,
        exluding when there are no links and when there are multiple (ie ambiguous) links.

        >>> lt.with_n_links().with_linked_values(
        ...     ibis._.record_id.arbitrary().name("record_id_other"),
        ... ).filter(ibis._.n_links == 1)
        ┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━┓
        ┃ record_id ┃ x      ┃ n_links ┃ record_id_other ┃
        ┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━┩
        │ int64     │ string │ int64   │ int64           │
        ├───────────┼────────┼─────────┼─────────────────┤
        │        50 │ b      │       1 │              51 │
        └───────────┴────────┴─────────┴─────────────────┘

        If no values are provided, we will by default add a column named `other`
        with all the values from other packed into an array of structs.

        >>> lt.with_linked_values()
        LinkedTable<3 records, 3 links>
        ┏━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ record_id ┃ x      ┃ other                                                      ┃
        ┡━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64     │ string │ array<struct<record_id: int64, y: float64>>                │
        ├───────────┼────────┼────────────────────────────────────────────────────────────┤
        │        40 │ a      │ [{'record_id': 41, 'y': 4.1}, {'record_id': 42, 'y': 4.2}] │
        │        50 │ b      │ [{'record_id': 51, 'y': 9.0}]                              │
        │        60 │ c      │ NULL                                                       │
        └───────────┴────────┴────────────────────────────────────────────────────────────┘
        """  # noqa: E501
        uname = _util.unique_name()
        if not values and not named_values:
            values = (
                lambda t: ibis.struct({c: t[c] for c in t.columns if c != uname}).name(
                    "other"
                ),
            )

        id_to_other = self.links_.select(
            **{
                uname: "record_id_l",
                "record_id": "record_id_r",
            }
        ).join(self.other_, "record_id")
        vals_bound = id_to_other.bind(*values, **named_values)
        aggs = [
            v if isinstance(v, ir.Scalar) else default_agg.resolve(v).name(v.get_name())
            for v in vals_bound
        ]

        self_id_to_other_array_vals = id_to_other.group_by(uname).agg(*aggs)
        value_names = [c for c in self_id_to_other_array_vals.columns if c != uname]

        t = self.mutate(_.record_id.name(uname))
        collisions = [c for c in value_names if c in t.columns]
        if collisions:
            raise ValueError(
                "Given values would collide with existing columns in self: "
                f"{collisions}",
            )
        with_other = _util.join_lookup(
            t,
            self_id_to_other_array_vals,
            uname,
        ).drop(uname)
        return self.__class__(with_other, other=self.other_, links=self.links_)

    def with_n_links(self, /, *, name: str = "n_links") -> _typing.Self:
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
        >>> import mismo
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"record_id": [4, 5, 6]})
        >>> right = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
        >>> linkage = mismo.Linkage(left=left, right=right, links=links)
        >>> linkage.left.with_n_links().order_by("record_id")
        ┏━━━━━━━━━━━┳━━━━━━━━━┓
        ┃ record_id ┃ n_links ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━┩
        │ int64     │ int64   │
        ├───────────┼─────────┤
        │         4 │       2 │
        │         5 │       1 │
        │         6 │       0 │
        └───────────┴─────────┘
        >>> linkage.right.with_n_links(name="link_count").order_by("record_id")
        ┏━━━━━━━━━━━┳━━━━━━━━━━━━┓
        ┃ record_id ┃ link_count ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━━━━┩
        │ int64     │ int64      │
        ├───────────┼────────────┤
        │         7 │          1 │
        │         8 │          1 │
        │         9 │          1 │
        └───────────┴────────────┘
        """
        # this doesn't include counts for ids that aren't in links
        # (eg those that aren't linked at all)
        # So make sure these get added back in during the join_lookup()
        n_by_id = self.links_.group_by(record_id="record_id_l").aggregate(
            _.record_id_r.nunique().name(name)
        )
        t = self
        if name in t.columns:
            t = t.drop(name)
        added = _util.join_lookup(t, n_by_id, "record_id", defaults={name: 0})
        return self.__class__(added, other=self.other_, links=self.links_)

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
        >>> import mismo
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"record_id": [4, 5, 6]})
        >>> right = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
        >>> linkage = mismo.Linkage(left=left, right=right, links=links)

        There is 1 record in left (6) that didn't match any in right.
        There is 1 record in left (5) that matched 1 in right.
        There is 1 record in left (4) that matched 2 in right.

        >>> linkage.left.link_counts().order_by("n_links")
        ┏━━━━━━━━━┳━━━━━━━━━━━┓
        ┃ n_links ┃ n_records ┃
        ┡━━━━━━━━━╇━━━━━━━━━━━┩
        │ int64   │ int64     │
        ├─────────┼───────────┤
        │       0 │         1 │
        │       1 │         1 │
        │       2 │         1 │
        └─────────┴───────────┘

        All 3 records in right matched 1 in left.

        >>> linkage.right.link_counts()
        ┏━━━━━━━━━┳━━━━━━━━━━━┓
        ┃ n_links ┃ n_records ┃
        ┡━━━━━━━━━╇━━━━━━━━━━━┩
        │ int64   │ int64     │
        ├─────────┼───────────┤
        │       1 │         3 │
        └─────────┴───────────┘
        """
        basic_counts = self.links_.group_by(record_id="record_id_l").aggregate(
            n_links=_.record_id_r.nunique()
        )
        zero_counts = self.filter(_.record_id.notin(basic_counts.record_id)).select(
            "record_id", n_links=ibis.literal(0, type="int64")
        )
        n_links_by_id = ibis.union(basic_counts, zero_counts)
        counts = n_links_by_id.group_by("n_links").aggregate(
            n_records=_.record_id.count()
        )
        return LinkCountsTable(counts)

    @classmethod
    def make_pair(
        cls, *, left: ir.Table, right: ir.Table, links: ir.Table
    ) -> tuple[_typing.Self, _typing.Self]:
        """
        Create a pair of LinkedTables from left, right, and links.

        This basically just wraps the logic to make it so that
        the the _l and _r suffixes in the links table are consistent.
        """

        left = cls(left, other=right, links=links)
        right = cls(right, other=left, links=LinksTable._swap_perspective(links))
        return left, right

    def __repr__(self) -> str:
        return f"""{self.__class__.__name__}<{self.count().execute()} records, {self.links_.count().execute()} links>
{super().__repr__()}
""".strip()  # noqa: E501


class LinkCountsTable(TableWrapper):
    """A table representing the number of records binned by number of links.

    eg "There were 700 records with 0 links, 300 with 1 link, 20 with 2 links, ..."
    """

    n_records: ir.IntegerColumn
    """The number of records."""
    n_links: ir.IntegerColumn
    """The number of links."""

    def __init__(self, t: ibis.Table) -> None:
        """Create from an ibis table with exactly columns 'n_records' and 'n_links'.

        Parameters
        ----------
        t
            The table with exactly columns 'n_records' and 'n_links'.
        """
        if set(t.columns) != {"n_records", "n_links"}:
            raise ValueError(
                "LinkCountsTable must have exactly columns 'n_records' and 'n_links'"
            )
        super().__init__(t)

    def chart(self) -> alt.Chart:
        """A bar chart of the number of records by the number of links.

        ```plaintext
                         Number of Records
        Number of          By Link Count
          Records
                |    █
        100,000 | █  █
                | █  █
                | █  █  █
         10,000 | █  █  █
                | █  █  █
                | █  █  █
          1,000 | █  █  █  █
                | █  █  █  █  █  █
                | █  █  █  █  █  █  █
            100 | █  █  █  █  █  █  █  █  █
                | 0  1  2  3  4 10 12 14 23
                Number of Links
        ```
        """
        import altair as alt

        total_records = self.n_records.sum().fill_null(0).execute()
        n_title = ["Number of Records", f"({total_records:_} total)"]
        key_title = "Number of Links"
        mins = self.order_by(_.n_links.asc()).limit(2).execute()
        if len(mins) > 1:
            subtitle = f"eg '{mins.n_records[0]:_} records had {mins.n_links[0]:_} links, {mins.n_records[1]:_} had {mins.n_links[1]:_} links, ...'"  # noqa: E501
        elif len(mins) == 1:
            subtitle = (
                f"eg '{mins.n_records[0]:_} records had {mins.n_links[0]:_} links', ..."  # noqa: E501
            )
        else:
            subtitle = "eg 'there were 1000 records with 0 links, 500 with 1 link, 100 with 2 links, ...'"  # noqa: E501

        t = self.mutate(
            frac_records=_.n_records / total_records if total_records > 0 else 0
        )
        chart = (
            alt.Chart(t)
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
                    alt.Tooltip(
                        "frac_records:Q", title="Fraction of Records", format=".2%"
                    ),
                    alt.Tooltip("n_links:O", title=key_title),
                ],
            )
        )
        return chart


def _n_links_by_id(ids: ir.Table, links: ir.Table) -> ibis.Table:
    n_links_by_id = links.group_by(record_id="record_id_l").aggregate(
        n_links=_.record_id_r.nunique()
    )
    # The above misses records with no entries in the links table (eg unlinked records)
    records_not_linked = (
        ids.distinct().filter(_.record_id.notin(links.record_id_l)).mutate(n_links=0)
    )
    # the default of 0 is an int8, which isn't unionable with the int64 of other table
    records_not_linked = records_not_linked.cast(n_links_by_id.schema())
    n_links_by_id = ibis.union(n_links_by_id, records_not_linked)
    return n_links_by_id
