from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar

import ibis

from mismo import _common, _typing
from mismo.types._linked_table import LinkedTable
from mismo.types._links_table import LinksTable

if TYPE_CHECKING:
    import altair as alt
    from ibis.expr import types as ir


class Linkage:
    """A dataclass of two Tables of records ([LinkedTables][mismo.LinkedTable]) and a Table of links ([LinksTable][mismo.LinksTable]) between them.

    Each record in `left` can be linked from 0 to N records in `right`, and vice versa.

    See Also
    --------
    The [Diff][mismo.Diff] dataclass, for representing the special case
    where each record is linked to at most one other record.
    """  # noqa: E501

    def __init__(
        self, *, left: ibis.Table, right: ibis.Table, links: ibis.Table
    ) -> None:
        """Create from two Tables and a Table of links between them.

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

    @classmethod
    def from_join_condition(
        cls,
        left: ibis.Table,
        right: ibis.Table,
        condition: Any,
    ) -> _typing.Self:
        """
        Create a Linkage from two Tables and a join condition.

        Parameters
        ----------
        left
            A Table of records, with at least a column 'record_id'.
        right
            A Table of records, with at least a column 'record_id'.
        condition
            A join condition, such as a boolean expression or an ibis expression.
            See [mismo.join_condition][] for more details.

        Returns
        -------
            A Linkage object.
        """
        links = LinksTable.from_join_condition(left, right, condition)
        return cls(left=left, right=right, links=links)

    def cache(self) -> _typing.Self:
        """Cache left, right, and links for faster subsequent access."""
        return self.__class__(
            left=self.left.cache(), right=self.right.cache(), links=self.links.cache()
        )

    def to_parquets(self, directory: str | Path, /, *, overwrite: bool = False) -> None:
        """
        Write left, right, and links to parquet files in the given directory.
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
        return f"{self.__class__.__name__}<left={self.left.count().execute():_}, right={self.right.count().execute():_}, links={self.links.count().execute():_}>"  # noqa: E501

    def copy(
        self,
        *,
        left: LinkedTable | None = None,
        right: LinkedTable | None = None,
        links: LinksTable | None = None,
    ) -> Linkage:
        """
        Create a new Linkage, optionally replacing the left, right, and links tables.
        """
        return self.__class__(
            left=left if left is not None else self.left,
            right=right if right is not None else self.right,
            links=links if links is not None else self.links,
        )

    def link_counts_chart(self) -> alt.Chart:
        """
        A side by side altair Chart of `left.link_counts().chart()` and `right.link_counts().chart()`.

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
        """  # noqa: E501
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


Linkish = TypeVar("T", bound=LinksTable | Linkage)


# TODO: IDK if this deserves to be its own function,
# or if this should just get covered in a HowTo guide,
# and users should implement it themselves.
def filter_links(links_or_linkage: Linkish, condition: ir.BooleanValue) -> Linkish:
    """
    Create a new Linkage/LinksTable, filtered by the given condition.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> links = ibis.memtable(
    ...     [
    ...         (1, "a", 0.45),
    ...         (1, "b", 0.67),
    ...         (2, "c", 0.23),
    ...         (2, "c", 0.87),
    ...         (3, "d", 0.12),
    ...         (4, "d", 0.97),
    ...     ],
    ...     schema={
    ...         "record_id_l": "int64",
    ...         "record_id_r": "string",
    ...         "score": "float64",
    ...     },
    ... )

    We only want to keep links that are above a certain score.

    >>> filter_links(links, ibis._.score > 0.5)
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ score   ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
    │ int64       │ string      │ float64 │
    ├─────────────┼─────────────┼─────────┤
    │           1 │ b           │    0.67 │
    │           2 │ c           │    0.87 │
    │           4 │ d           │    0.97 │
    └─────────────┴─────────────┴─────────┘

    Or, say we are doing a lookup into a clean table (left) from a
    new set of dirty data (right).
    We want to only include links that are unambiguous,
    eg where each record in right is linked to at most one record in left.

    >>> filter_links(
    ...     links, (ibis._.record_id_l.nunique() == 1).over(group_by="record_id_r")
    ... ).order_by("score")
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ score   ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
    │ int64       │ string      │ float64 │
    ├─────────────┼─────────────┼─────────┤
    │           2 │ c           │    0.23 │
    │           1 │ a           │    0.45 │
    │           1 │ b           │    0.67 │
    │           2 │ c           │    0.87 │
    └─────────────┴─────────────┴─────────┘

    Or, see how there are two links between 2 and c.
    We only want to keep the one with the highest score, per each record in right.

    >>> filter_links(
    ...     links, (ibis._.score == ibis._.score.max()).over(group_by="record_id_r")
    ... ).order_by("score")
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ score   ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
    │ int64       │ string      │ float64 │
    ├─────────────┼─────────────┼─────────┤
    │           1 │ a           │    0.45 │
    │           1 │ b           │    0.67 │
    │           2 │ c           │    0.87 │
    │           4 │ d           │    0.97 │
    └─────────────┴─────────────┴─────────┘

    Or, perhaps say we have many different linking methods,
    eg link where the names match, or where the addresses match.
    We only want to keep links where at least two of the linking methods agree.

    >>> filter_links(
    ...     links,
    ...     (ibis._.count() >= 2).over(group_by=("record_id_l", "record_id_r")),
    ... )
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ score   ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━┩
    │ int64       │ string      │ float64 │
    ├─────────────┼─────────────┼─────────┤
    │           2 │ c           │    0.23 │
    │           2 │ c           │    0.87 │
    └─────────────┴─────────────┴─────────┘
    """  # noqa: E501
    if isinstance(links_or_linkage, Linkage):
        return links_or_linkage.copy(
            links=filter_links(links_or_linkage.links, condition)
        )
    else:
        return links_or_linkage.filter(condition)
