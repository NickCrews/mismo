from __future__ import annotations

import functools
from textwrap import dedent
from typing import Literal
import warnings

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util


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
