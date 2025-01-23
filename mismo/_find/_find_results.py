from __future__ import annotations

import functools
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Literal
import warnings

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util

if TYPE_CHECKING:
    import altair as alt


class LinkCountsTable(_util.TableWrapper):
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


class LabeledTable(ibis.Table):
    """
    A table with a `record_ids` column added of the link(s) to the other table.

    This represents one of the input tables, augmented with another column
    that points to the best link(s) in the other table.
    """

    record_id: ir.Column
    """The record_id of this table."""
    record_ids: ir.ArrayColumn
    """The record_ids of the links in the other table. Never NULL, if no links, empty array."""  # noqa: E501


class LinkTable(ibis.Table):
    """
    A table with a `record_id_l` and `record_id_r` column, representing linked pairs.
    """

    record_id_l: ir.Column
    record_id_r: ir.Column


class FindResults:
    """A Dataclass representing the results of a find operation.

    For example, you have an existing database that is clean, and you want
    to ingest some new, messy data.
    For each of the records in the new, messy data, you want to find
    the linked records in the existing database.

    The `haystack` is the existing database, the `needle` is the new data,
    and the `links` are the relationships between the two.
    """

    def __init__(
        self, *, haystack: ir.Table, needle: LabeledTable, links: LinkTable
    ) -> None:
        """Create a new FindResults object.

        Parameters
        ----------
        haystack
            The table being searched. Must contain a `record_id` column.
        needle
            The table being searched for. Must contain a `record_id` column.
        links
            Table of (recod_id_l, record_id_r) representing links between
            the needle and the haystack.

            !!! warning
            `record_id_l` is the record_id of the haystack,
            `record_id_r` is the record_id of the needle.
        """
        self._haystack = haystack
        self._needle = needle
        self._links = links

    def haystack(self) -> ir.Table:
        """The table being searched."""
        return self._haystack

    def needle(self) -> ir.Table:
        """The table being searched for."""
        return self._needle

    def links(self) -> ir.Table:
        """
        The (record_id_l, record_id_r) pairs referencing the needle and the haystack.
        """
        return self._links

    def to_parquets(self, directory: str | Path, /) -> None:
        """
        Write the needle, haystack, and links to parquet files in the given directory.
        """
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        self.haystack().to_parquet(d / "haystack.parquet")
        self.needle().to_parquet(d / "needle.parquet")
        self.links().to_parquet(d / "links.parquet")

    @classmethod
    def from_parquets(
        cls, directory: str | Path, /, backend: ibis.BaseBackend | None = None
    ) -> None:
        """Create a FindResults by reading parquets from the given directory."""
        if backend is None:
            backend = ibis
        d = Path(directory)
        return cls(
            haystack=backend.read_parquet(d / "haystack.parquet"),
            needle=backend.read_parquet(d / "needle.parquet"),
            links=backend.read_parquet(d / "links.parquet"),
        )

    def needle_labeled(self, *, name: str = "record_ids") -> LabeledTable:
        """The needle, with a `record_ids` column added of links from the haystack."""
        n = self.needle()
        if name in n.columns:
            warnings.warn(f"Column '{name}' will be overwritten in needle.")
            n = n.drop(name)

        lookup = (
            self.links()
            .group_by(record_id=_.record_id_r)
            .agg(_.record_id_l.collect().name(name))
        )
        return _util.join_lookup(n, lookup, "record_id", defaults={name: []})

    def needle_labeled_none(self) -> LabeledTable:
        """The subset of needle_labeled with no links."""
        return self.needle_labeled().filter(_.record_ids.length() == 0)

    def needle_labeled_single(
        self,
        *,
        name: str = "record_id",
        format: Literal["single", "array"] = "single",
    ) -> ir.Table:
        """The subset of needle_labeled with exactly one link.

        Parameters
        ----------
        name:
            How to name the column.
        format
            - "single": Return a table with a `record_id` column, without the `record_ids` column.
            - "array": Return the table as-is.
        """  # noqa: E501
        raw = self.needle_labeled().filter(_.record_ids.length() == 1)
        if format == "single":
            return raw.mutate(_.record_ids[0].name(name)).drop("record_ids")
        elif format == "array":
            return raw
        else:
            raise ValueError(f"format must be 'single' or 'array', got {format:!r}")

    def needle_labeled_many(self) -> LabeledTable:
        """The subset of needle_labeled with more than one link."""
        return self.needle_labeled().filter(_.record_ids.length() > 1)

    def needle_link_counts(self) -> LinkCountsTable:
        """
        A histogram of the number of records in the needle, binned by number of links.

        e.g. "There were 700 records with 0 links, 300 with 1 link,
        20 with 2 links, ..."
        """
        n_links_by_record = self.links().record_id_r.value_counts(name="n_links")
        counts = n_links_by_record.group_by("n_links").agg(n_records=_.count())
        n_not_linked = (
            self.needle()
            .select("record_id")
            .distinct()
            .filter(_.record_id.notin(self.links().record_id_r))
            .count()
        )
        extra = (
            n_not_linked.name("n_records")
            .as_table()
            .mutate(n_links=0)
            .cast(counts.schema())
        )
        counts = counts.union(extra)
        counts = counts.order_by(_.n_links)
        return LinkCountsTable(counts)

    def haystack_labeled(self, *, name: str = "record_ids") -> LabeledTable:
        """The haystack, with a `record_ids` column added of links from the needle."""
        h = self.haystack()
        if name in h.columns:
            warnings.warn(f"Column '{name}' will be overwritten in haystack.")
            h = h.drop(name)

        lookup = (
            self.links()
            .group_by(record_id=_.record_id_l)
            .agg(_.record_id_r.collect().name(name))
        )
        return _util.join_lookup(h, lookup, "record_id", defaults={name: []})

    def haystack_labeled_none(self) -> LabeledTable:
        """The subset of haystack with no links."""
        return self.haystack_labeled().filter(_.record_ids.length() == 0)

    def haystack_labeled_single(
        self,
        *,
        name: str = "record_id",
        format: Literal["single", "array"] = "single",
    ) -> ir.Table:
        """The subset of haystack_labeled with exactly one link.

        Parameters
        ----------
        name:
            How to name the column.
        format
            - "single": Return a table with a {name} column, without the `record_ids` column.
            - "array": Return the table as-is.
        """  # noqa: E501
        raw = self.haystack_labeled().filter(_.record_ids.length() == 1)
        if format == "single":
            return raw.mutate(_.record_ids[0].name(name)).drop("record_ids")
        elif format == "array":
            return raw
        else:
            raise ValueError(f"format must be 'single' or 'array', got {format:!r}")

    def haystack_labeled_many(self) -> LabeledTable:
        """The subset of haystack with more than one link."""
        return self.haystack_labeled().filter(_.record_ids.length() > 1)

    def haystack_link_counts(self) -> LinkCountsTable:
        """
        A histogram of the records in the haystack, binned by number of links.

        e.g. "There were 700 records with 0 links, 300 with 1 link,
        20 with 2 links, ..."
        """
        n_links_by_record = self.links().record_id_l.value_counts(name="n_links")
        counts = n_links_by_record.group_by("n_links").agg(n_records=_.count())
        n_not_linked = (
            self.haystack()
            .select("record_id")
            .distinct()
            .filter(_.record_id.notin(self.links().record_id_l))
            .count()
        )
        extra = (
            n_not_linked.name("n_records")
            .as_table()
            .mutate(n_links=0)
            .cast(counts.schema())
        )
        counts = counts.union(extra)
        counts = counts.order_by(_.n_links)
        return LinkCountsTable(counts)

    @functools.cache
    def __str__(self) -> str:
        return dedent(
            f"""
        FindResults(
            haystack.count()={self.haystack().count().execute():,},
            haystack_labeled_none().count()={self.haystack_labeled_none().count().execute():,},
            haystack_labeled_single().count()={self.haystack_labeled_single().count().execute():,},
            haystack_labeled_many().count()={self.haystack_labeled_many().count().execute():,},
            needle.count()={self.needle().count().execute():,},
            needle_labeled.count()={self.needle_labeled().count().execute():,})
            needle_labeled_none.count()={self.needle_labeled_none().count().execute():,},
            needle_labeled_single().count()={self.needle_labeled_single().count().execute():,},
            needle_labeled_many().count()={self.needle_labeled_many().count().execute():,},
        )
        """.strip()
        )

    def __repr__(self) -> str:
        return self.__str__()
