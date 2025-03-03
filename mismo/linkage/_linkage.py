from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING

import ibis

from mismo import _common, _typing
from mismo.types._linked_table import LinkedTable
from mismo.types._links_table import LinksTable

if TYPE_CHECKING:
    import altair as alt
    from ibis.expr import types as ir


class BaseLinkage(abc.ABC):
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

    @property
    @abc.abstractmethod
    def left(self) -> LinkedTable:
        """The left Table."""

    @property
    @abc.abstractmethod
    def right(self) -> LinkedTable:
        """The right Table."""

    @property
    @abc.abstractmethod
    def links(self) -> LinksTable:
        """
        A table of (record_id_l, record_id_r, <other attributes>...) that link `left` and `right`.
        """  # noqa: E501

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

    @abc.abstractmethod
    def cache(self) -> _typing.Self:
        """
        Cache the left, right, and links tables.

        Returns
        -------
        A new Linkage with the cached tables.
        """
        raise NotImplementedError


class LinkTableLinkage(BaseLinkage):
    """A Linkage backed by a table of links.

    This is the simplest kind of linkage, but also has some of the most restrictions.
    For example, if you try to cache this linkage, then the backend must actually
    store all the links to a physical table, and the links table might be very large.
    """

    def __init__(self, left: ibis.Table, right: ibis.Table, links: ibis.Table) -> None:
        """Create from two tables and a table of links between them.

        Parameters
        ----------
        left
            A Table of records, with at least a column 'record_id'.
        right
            A Table of records, with at least a     column 'record_id'.
        links
            A Table of links between the two tables.
            Must have columns 'record_id_l' and 'record_id_r', which refer to the
            'record_id' columns in `left` and `right`, respectively.
            May have other columns.
            May not have duplicate (record_id_l, record_id_r) pairs.
        """
        _common.check_tables_and_links(left, right, links)

        self._left, self._right = LinkedTable.make_pair(
            left=left, right=right, links=links
        )
        self._raw_links = links

    @property
    def left(self) -> LinkedTable:
        """The left Table."""
        return self._left

    @property
    def right(self) -> LinkedTable:
        """The right Table."""
        return self._right

    @property
    def links(self) -> LinksTable:
        """
        A table of (record_id_l, record_id_r, <other attributes>...) that link `left` and `right`.
        """  # noqa: E501
        return LinksTable(self._raw_links, left=self._left, right=self._right)

    def cache(self) -> _typing.Self:
        """Cache this Linkage for faster subsequent access."""
        return LinkTableLinkage(
            left=self.left.cache(),
            right=self.right.cache(),
            links=self.links.cache(),
        )

    def to_parquets(self, directory: str | Path, /, *, overwrite: bool = False) -> None:
        """
        Write the needle, haystack, and links to parquet files in the given directory.
        """
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)

        def write(t: ir.Table, name: str):
            p = d / f"{name}.parquet"
            if p.exists() and not overwrite:
                raise FileExistsError(f"{p} already exists")
            t.to_parquet(p)

        write(self.left, "left")
        write(self.right, "right")
        write(self.links, "links")

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

    def __repr__(self):
        return f"""
{self.__class__.__name__}(
    left={self.left.count().execute():_},
    right={self.right.count().execute():_},
    links={self.links.count().execute():_},
)
""".strip()
