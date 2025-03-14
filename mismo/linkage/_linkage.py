from __future__ import annotations

import abc
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

import ibis

from mismo import _common, _typing
from mismo.types._linked_table import LinkedTable
from mismo.types._links_table import LinksTable

if TYPE_CHECKING:
    import altair as alt
    from ibis.expr import types as ir


@runtime_checkable
class Linkage(Protocol):
    """A simple dataclass containing two tables of records ([LinkedTables][mismo.LinkedTable]) and links ([LinksTable][mismo.LinksTable]) between them.

    This is a protocol, you as a user will interact with concrete implementations
    of a Linkage, such as a [LinkTableLinkage][mismo.LinkTableLinkage]
    or a [KeyLinkage][mismo.KeyLinkage].

    See Also
    --------
    The [Diff][mismo.Diff] dataclass, for representing the special case
    where each record is linked to at most one other record.
    """  # noqa: E501

    @property
    def left(self) -> LinkedTable:
        """The left Table."""
        raise NotImplementedError

    @property
    def right(self) -> LinkedTable:
        """The right Table."""
        raise NotImplementedError

    @property
    def links(self) -> LinksTable:
        """
        A table of (record_id_l, record_id_r, <other attributes>...) that link `left` and `right`.
        """  # noqa: E501
        raise NotImplementedError

    def adjust(
        self,
        *,
        left: LinkedTable | None = None,
        right: LinkedTable | None = None,
        links: LinksTable | None = None,
    ) -> Linkage:
        """
        Create a new Linkage, optionally replacing the left, right, and links tables.

        The result is not guaranteed to be the same type as the original Linkage.
        For example, if the original Linkage is a [KeyLinkage][mismo.KeyLinkage],
        then if you adjust the links table, it would be impossible for the
        result to be represented as a KeyLinkage, so we return a LinkTableLinkage
        """
        raise NotImplementedError

    def link_counts_chart(self) -> alt.Chart:
        """
        A side by side altair Chart of `left.link_counts()` and `right.link_counts()`

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
        raise NotImplementedError

    def cache(self) -> _typing.Self:
        """
        Cache the left, right, and links tables.

        Returns
        -------
        A new Linkage with the cached tables.
        """
        raise NotImplementedError


class BaseLinkage(abc.ABC, Linkage):
    """
    An abstract base class provided as convenience for those implementing Linkage's.
    """

    @property
    @abc.abstractmethod
    def left(self) -> LinkedTable:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def right(self) -> LinkedTable:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def links(self) -> LinksTable:
        raise NotImplementedError

    def link_counts_chart(self) -> alt.Chart:
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

    def adjust(
        self,
        *,
        left: LinkedTable | None = None,
        right: LinkedTable | None = None,
        links: LinksTable | None = None,
    ) -> Linkage:
        return self.__class__(
            left=left if left is not None else self.left,
            right=right if right is not None else self.right,
            links=links if links is not None else self.links,
        )

    @abc.abstractmethod
    def cache(self) -> _typing.Self:
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
