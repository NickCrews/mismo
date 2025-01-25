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

    Each record here can be linked to 0-N records in the `other_`.

    This acts just like an Ibis Table, but it has a few extra attributes
    and methods that make it more ergonomic to work with,
    eg to add data from the linked table.
    """

    other_: ibis.Table
    """The other table that this table is linked to.
    Trailing underscore to avoid name conflicts with column names."""
    links_: ibis.Table
    """The table of links between this table and `other`.
    Trailing underscore to avoid name conflicts with column names.
    """

    def __init__(self, table: ibis.Table, other: ibis.Table, links: ibis.Table):
        _check_tables_and_links(table, other, links)

        super().__init__(table)
        object.__setattr__(self, "other_", other)
        object.__setattr__(self, "links_", links)

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
        >>> this = ibis.memtable({"record_id": [4, 5, 6]})
        >>> other = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
        >>> lt = LinkedTable(this, other, links)
        >>> lt
        ┏━━━━━━━━━━━┓
        ┃ record_id ┃
        ┡━━━━━━━━━━━┩
        │ int64     │
        ├───────────┤
        │         4 │
        │         5 │
        │         6 │
        └───────────┘

        Get the "idr" values from all the linked records,
        as well as create a derived value "plus_one" that is "idr" + 1:

        >>> lt.with_many_linked_values(plus_one=_.record_id + 1)
        ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ record_id ┃ plus_one             ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64     │ array<int64>         │
        ├───────────┼──────────────────────┤
        │         4 │ [8, 9]               │
        │         5 │ [10]                 │
        │         6 │ []                   │
        └───────────┴──────────────────────┘

        Default is to pack everything into array<struct<all columns from other>>:

        >>> lt.with_many_linked_values()
        ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ record_id ┃ other                                ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
        │ int64     │ array<struct<record_id: int64>>      │
        ├───────────┼──────────────────────────────────────┤
        │         4 │ [{'record_id': 7}, {'record_id': 8}] │
        │         5 │ [{'record_id': 9}]                   │
        │         6 │ []                                   │
        └───────────┴──────────────────────────────────────┘
        """
        if not values and not named_values:
            values = (
                lambda t: ibis.struct({c: t[c] for c in t.columns}).name("other"),
            )

        uname = _util.unique_name()
        o = self.other_.select(_.record_id.name(uname), *values, **named_values)
        id_to_other_vals = (
            self.links_.select(
                **{
                    "record_id": "record_id_l",
                    uname: "record_id_r",
                }
            )
            .join(o, uname)
            .drop(uname)
        )
        value_names = [c for c in id_to_other_vals.columns if c != "record_id"]
        self_id_to_other_array_vals = id_to_other_vals.group_by("record_id").agg(
            *[_[c].collect().name(c) for c in value_names]
        )

        t = self
        need_to_drop = [c for c in value_names if c in t.columns]
        t = t.drop(*need_to_drop)
        with_other = _util.join_lookup(
            t,
            self_id_to_other_array_vals,
            "record_id",
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
        >>> this = ibis.memtable({"record_id": [4, 5, 6]})
        >>> other = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
        >>> lt = LinkedTable(this, other, links)
        >>> lt
        ┏━━━━━━━━━━━┓
        ┃ record_id ┃
        ┡━━━━━━━━━━━┩
        │ int64     │
        ├───────────┤
        │         4 │
        │         5 │
        │         6 │
        └───────────┘

        Get the "idr" value from the linked record,
        as well as create a derived value "plus_one" that is "idr" + 1:

        >>> lt.with_single_linked_values(plus_one=_.record_id + 1)
        ┏━━━━━━━━━━━┳━━━━━━━━━━┓
        ┃ record_id ┃ plus_one ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━━┩
        │ int64     │ int64    │
        ├───────────┼──────────┤
        │         5 │       10 │
        └───────────┴──────────┘

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
        o = self.other_.select(_.record_id.name(uname), *values, **named_values)
        id_to_other_vals = (
            self.links_.select(
                **{
                    "record_id": "record_id_l",
                    uname: "record_id_r",
                }
            )
            .join(o, uname)
            .drop(uname)
        )

        need_to_drop = [
            c for c in id_to_other_vals.columns if c in t.columns and c != "record_id"
        ]
        t = t.drop(*need_to_drop)
        with_other = _util.join_lookup(t, id_to_other_vals, "record_id")
        return self.__class__(with_other, self.other_, self.links_)

    def _n_links_by_id(self) -> ir.Table:
        return _n_links_by_id(self.select("record_id"), self.links_)

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
        >>> left = ibis.memtable({"record_id": [4, 5, 6]})
        >>> right = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
        >>> linkage = Linkage(left, right, links)
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
        >>> linkage.right.with_n_links().order_by("record_id")
        ┏━━━━━━━━━━━┳━━━━━━━━━┓
        ┃ record_id ┃ n_links ┃
        ┡━━━━━━━━━━━╇━━━━━━━━━┩
        │ int64     │ int64   │
        ├───────────┼─────────┤
        │         7 │       1 │
        │         8 │       1 │
        │         9 │       1 │
        └───────────┴─────────┘
        """
        n_by_id = self._n_links_by_id().rename(**{name: "n_links"})
        t = self
        if name in t.columns:
            t = t.drop(name)
        added = _util.join_lookup(t, n_by_id, "record_id")
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
        >>> left = ibis.memtable({"record_id": [4, 5, 6]})
        >>> right = ibis.memtable({"record_id": [7, 8, 9]})
        >>> links = ibis.memtable({"record_id_l": [4, 4, 5], "record_id_r": [7, 8, 9]})
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
            self._n_links_by_id()
            .n_links.value_counts(name="n_records")
            .order_by(_.n_links.desc())
            .select("n_records", "n_links")
        )
        return LinkCountsTable(counts)


class Linkage:
    """Two tables of records and links between them.

    This is semantically similar to a [Diff][mismo.types.Diff] object,
    except in a Diff object every row in `left` is linked
    to either 0 or 1 rows in `right`.
    Because there can't be many-to- relationships,
    Diffs allow for the semantics of insertions, updates, and deletions.
    eg "this row changed in these ways between these two tables".

    On the other hand, a Linkage object is more general.
    It supports the semantics of a many-to-many relationship between two tables.
    Say you have a clean database of records.
    You just got a new batch of dirty data that might contain duplicates.
    Each record in the clean database might match multiple records in the dirty data.
    This makes it difficult to use a Diff object, because each clean record
    can't be paired up nicely with a single dirty record.
    A Linkage object is more appropriate in this case.
    """

    def __init__(self, left: ibis.Table, right: ibis.Table, links: ibis.Table) -> None:
        """Create from two tables and a table of links between them.

        Parameters
        ----------
        left
            A Table of records, with at least a column 'record_id'.
        right
            A Table of records, with at least a column 'record_id'.
        links
            A Table of links between the two tables.
            Must have columns 'record_id_l' and 'record_id_r', which refer to the
            'record_id' columns in `left` and `right`, respectively.
            May have other columns.
            May not have duplicate (record_id_l, record_id_r) pairs.
        """
        _check_tables_and_links(left, right, links)

        self._left = LinkedTable(left, right, links)
        self._right = LinkedTable(
            right,
            left,  # TODO: is this a huge footgun since the other cols aren't renamed??
            links.rename(record_id_l="record_id_r", record_id_r="record_id_l"),
        )
        self._links = links

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
        """
        A table of (record_id_l, record_id_r, <other attributes>...) that link `left` and `right`.
        """  # noqa: E501
        return self._links

    @classmethod
    def from_predicates(
        cls,
        left: ibis.Table,
        right: ibis.Table,
        predicates,
    ) -> _typing.Self:
        """
        Create a Linkage from join predicates.

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
        >>> linkage = Linkage.from_predicates(tl, tr, "record_id")
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
        if "record_id" not in left.columns:
            left = left.mutate(ibis.row_number().name("record_id"))
        if "record_id" not in right.columns:
            right = right.mutate(ibis.row_number().name("record_id"))
        links = ibis.join(
            left,
            right,
            predicates,
            lname="{name}_l",
            rname="{name}_r",
        )
        links = _util.ensure_join_suffixed(left.columns, right.columns, links)
        links = links.select("record_id_l", "record_id_r")
        return cls(left, right, links)

    def link_counts_chart(self) -> alt.Chart:
        """
        A side by side altair Chart of `left.link_counts(`)` and `right.link_counts()`

        ```plaintext
        Number of           Left Table               Number of    Right Table
          Records                                      Records
                |    █                                       |    █
        100,000 | █  █                                       |    █
                | █  █                                10,000 |    █
                | █  █  █                                    |    █
         10,000 | █  █  █                                    |    █  █
                | █  █  █                                    | █  █  █
                | █  █  █                              1,000 | █  █  █
          1,000 | █  █  █  █                                 | █  █  █
                | █  █  █  █  █  █                           | █  █  █
                | █  █  █  █  █  █  █                        | █  █  █  █
            100 | █  █  █  █  █  █  █  █  █              100 | █  █  █  █
                | 0  1  2  3  4 10 12 14 23                  | 0  1  2  3
                Number of Links                              Number of Links
        ```
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
        """Create a Linkage by reading parquets from the given directory."""
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


def _check_tables_and_links(
    left: ibis.Table, right: ibis.Table, links: ibis.Table
) -> None:
    if "record_id" not in left.columns:
        raise ValueError("column 'record_id' not in table")
    if "record_id" not in right.columns:
        raise ValueError("column 'record_id' not in other")
    if "record_id_l" not in links.columns:
        raise ValueError("column 'record_id_l' not in links")
    if "record_id_r" not in links.columns:
        raise ValueError("column 'record_id_r' not in links")
    try:
        left.record_id == links.record_id_l
    except Exception:
        raise ValueError(
            f"left.record_id of type {left.record_id.type()} is not comparable with links.record_id_l of type {links.record_id_l.type()}"  # noqa: E501
        )
    try:
        right.record_id == links.record_id_r
    except Exception:
        raise ValueError(
            f"right.record_id of type {right.record_id.type()} is not comparable with links.record_id_r of type {links.record_id_r.type()}"  # noqa: E501
        )
