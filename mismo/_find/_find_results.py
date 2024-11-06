from __future__ import annotations

import functools
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
    """A table representing the number of records by the number of matches.

    eg "There were 700 records with 0 matches, 300 with 1 match, 20 with 2 matches, ..."
    """

    n: ir.IntegerColumn
    """The number of records."""
    n_matches: ir.IntegerColumn
    """The number of matches."""

    def chart(self) -> alt.Chart:
        """A bar chart of the number of records by the number of matches."""
        import altair as alt

        n_title = "Number of Records"
        key_title = "Number of Matches"
        mins = self.order_by(_.n.desc()).limit(2).execute()
        if len(mins) > 1:
            subtitle = f"eg '{mins.n[0]:,} records had {mins.n_matches[0]} matches, {mins.n[1]:,} had {mins.n_matches[1]} matches, ...'"  # noqa: E501
        elif len(mins) == 1:
            subtitle = (
                f"eg '{mins.n[0]:,} records had {mins.n_matches[0]} matches', ..."
            )
        else:
            subtitle = "eg 'there were 1000 records with 0 matches, 500 with 1 match, 100 with 2 matches, ...'"  # noqa: E501
        chart = (
            alt.Chart(self)
            .properties(
                title=alt.TitleParams(
                    "Number of Records by Match Count",
                    subtitle=subtitle,
                    anchor="middle",
                ),
                width=alt.Step(12) if self.count().execute() <= 20 else alt.Step(8),
            )
            .mark_bar()
            .encode(
                alt.X("n_matches:O", title=key_title, sort="x"),
                alt.Y(
                    "n:Q",
                    title=n_title,
                    scale=alt.Scale(type="symlog"),
                ),
                tooltip=[
                    alt.Tooltip("n:Q", title=n_title, format=","),
                    alt.Tooltip("n_matches:O", title=key_title),
                ],
            )
        )
        return chart


class LabeledTable(ibis.Table):
    """
    A table with a `record_ids` column added of the match(es) from the other table.

    This represents one of the input tables, augmented with another column
    that points to the best match(es) in the other table.
    """

    record_id: ir.Column
    """The record_id of this table."""
    record_ids: ir.ArrayColumn
    """The record_ids of the matches in the other table. Never NULL, if no matches, empty array."""  # noqa: E501


class LinkTable(ibis.Table):
    """
    A table with a `record_id_l` and `record_id_r` column, representing matching pairs.
    """

    record_id_l: ir.Column
    record_id_r: ir.Column


class FindResults:
    """A Dataclass representing the results of a find operation.

    For example, you have an existing database that is clean, and you want
    to ingest some new, messy data.
    For each of the records in the new, messy data, you want to find
    the matches in the existing database.

    The `haystack` is the existing database, the `needle` is the new data,
    and the `links` are the pairs of matches between the two.
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
            The pairs of matches between the needle and the haystack.

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
        """The pairs of matches between the needle and the haystack."""
        return self._links

    def needle_labeled(self) -> LabeledTable:
        """The needle, with a `record_ids` column added of matches from the haystack."""
        n = self.needle()
        if "record_ids" in n.columns:
            warnings.warn("Column 'record_ids' will be overwritten in needle.")
            n = n.drop("record_ids")

        lookup = (
            self.links()
            .group_by(record_id=_.record_id_r)
            .agg(record_ids=_.record_id_l.collect())
        )
        return _util.join_lookup(n, lookup, "record_id", defaults={"record_ids": []})

    def needle_labeled_none(self) -> LabeledTable:
        """The subset of needle_labeled with no matches."""
        return self.needle_labeled().filter(_.record_ids.length() == 0)

    def needle_labeled_single(
        self, *, format: Literal["single", "array"] = "single"
    ) -> ir.Table:
        """The subset of needle_labeled with exactly one match.

        Parameters
        ----------
        format
            - "single": Return a table with a `record_id` column, without the `record_ids` column.
            - "array": Return the table as-is.
        """  # noqa: E501
        raw = self.needle_labeled().filter(_.record_ids.length() == 1)
        if format == "single":
            return raw.mutate(record_id=_.record_ids[0]).drop("record_ids")
        elif format == "array":
            return raw
        else:
            raise ValueError(f"format must be 'single' or 'array', got {format:!r}")

    def needle_labeled_many(self) -> LabeledTable:
        """The subset of needle_labeled with more than one match."""
        return self.needle_labeled().filter(_.record_ids.length() > 1)

    def needle_match_counts(self) -> LinkCountsTable:
        """
        A histogram of the number of records in the needle, binned by number of matches.

        e.g. "There were 700 records with 0 matches, 300 with 1 match,
        20 with 2 matches, ..."
        """
        n_matches_by_record = self.links().record_id_r.value_counts(name="n")
        counts = n_matches_by_record.group_by(n_matches=_.n).agg(n=_.count())
        n_not_linked = (
            self.needle()
            .select("record_id")
            .distinct()
            .filter(_.record_id.notin(self.links().record_id_r))
            .count()
        )
        extra = (
            n_not_linked.name("n").as_table().mutate(n_matches=0).cast(counts.schema())
        )
        counts = counts.union(extra)
        counts = counts.order_by(_.n_matches)
        return LinkCountsTable(counts)

    def haystack_labeled(self) -> LabeledTable:
        """The haystack, with a `record_ids` column added of matches from the needle."""
        h = self.haystack()
        if "record_ids" in h.columns:
            warnings.warn("Column 'record_ids' will be overwritten in haystack.")
            h = h.drop("record_ids")

        lookup = (
            self.links()
            .group_by(record_id=_.record_id_l)
            .agg(record_ids=_.record_id_r.collect())
        )
        return _util.join_lookup(h, lookup, "record_id", defaults={"record_ids": []})

    def haystack_labeled_none(self) -> LabeledTable:
        """The subset of haystack with no matches."""
        return self.haystack_labeled().filter(_.record_ids.length() == 0)

    def haystack_labeled_single(
        self, *, format: Literal["single", "array"] = "single"
    ) -> ir.Table:
        """The subset of haystack_labeled with exactly one match.

        Parameters
        ----------
        format
            - "single": Return a table with a `record_id` column, without the `record_ids` column.
            - "array": Return the table as-is.
        """  # noqa: E501
        raw = self.haystack_labeled().filter(_.record_ids.length() == 1)
        if format == "single":
            return raw.mutate(record_id=_.record_ids[0]).drop("record_ids")
        elif format == "array":
            return raw
        else:
            raise ValueError(f"format must be 'single' or 'array', got {format:!r}")

    def haystack_labeled_many(self) -> LabeledTable:
        """The subset of haystack with more than one match."""
        return self.haystack_labeled().filter(_.record_ids.length() > 1)

    def haystack_match_counts(self) -> LinkCountsTable:
        """
        A histogram of the records in the haystack, binned by number of matches.

        e.g. "There were 700 records with 0 matches, 300 with 1 match,
        20 with 2 matches, ..."
        """
        n_matches_by_record = self.links().record_id_l.value_counts(name="n")
        counts = n_matches_by_record.group_by(n_matches=_.n).agg(n=_.count())
        n_not_linked = (
            self.haystack()
            .select("record_id")
            .distinct()
            .filter(_.record_id.notin(self.links().record_id_l))
            .count()
        )
        extra = (
            n_not_linked.name("n").as_table().mutate(n_matches=0).cast(counts.schema())
        )
        counts = counts.union(extra)
        counts = counts.order_by(_.n_matches)
        return LinkCountsTable(counts)

    @functools.cache
    def __str__(self) -> str:
        return dedent(
            f"""
        FindResults(
            haystack.count()={self.haystack().count().execute():,},
            haystack_labeled_none().count()={self.haystack_labeled_none().count().execute():,},
            haystack_labeled_single().count()={self.haystack_labeled_single().count().execute():,},
            haystack_labeled_any().count()={self.haystack_labeled_single().count().execute():,},
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
