from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import ibis
from ibis import _

from mismo import _typing, _util, joins
from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    from mismo.types._linked_table import LinkedTable


class LinksTable(TableWrapper):
    """A table of links between two tables.

    This acts like an ibis table, guaranteed to have at least the
    columns record_id_l and record_id_r.
    It may have more columns, such as `address_match_level`,
    that describe the relationship between two records.

    In addition to the columns, this table has two properties, `left_` and `right_`,
    each of which is a [LinkedTable][mismo.LinkedTable] object,
    which is a wrapper around the left and right tables respectively.
    """

    # TODO:
    # Need to define whether or not it is allowed to have more than
    # one link between the same pair of records.

    record_id_l: ibis.Column
    """The record_id of the left table."""
    record_id_r: ibis.Column
    """The record_id of the right table."""

    def __init__(
        self, links: ibis.Table, *, left: ibis.Table, right: ibis.Table
    ) -> None:
        if "record_id_l" not in links.columns:
            raise ValueError("column 'record_id_l' not in links")
        if "record_id_r" not in links.columns:
            raise ValueError("column 'record_id_r' not in links")
        super().__init__(links)
        object.__setattr__(self, "_left_raw", left)
        object.__setattr__(self, "_right_raw", right)

    @classmethod
    def from_join_condition(
        cls,
        left: ibis.Table,
        right: ibis.Table,
        condition: Any,
    ) -> _typing.Self:
        if "record_id" not in left.columns:
            left = left.mutate(record_id=ibis.row_number())
        if "record_id" not in right.columns:
            right = right.mutate(record_id=ibis.row_number())
        links_raw = joins.join(
            left,
            right,
            condition,
            lname="{name}_l",
            rname="{name}_r",
            rename_all=True,
        )
        return LinksTable(links_raw, left=left, right=right)

    def with_left(
        self,
        *values: ibis.Deferred | Callable[[ibis.Table], ibis.Value] | None,
        **named_values: ibis.Deferred | Callable[[ibis.Table], ibis.Value] | None,
    ) -> LinksTable:
        """Add columns from the left table to this table of links.

        This allows you to add specific columns from the left table,
        renaming or modifying them as needed, following the `ibis.Table` pattern of
        `my_table.select("my_col", new_col=my_table.foo)`,
        except here we choose from the left table.

        Parameters
        ----------
        values
            The columns to add from the left table.
            Support string names, Deferreds, etc, just like `ibis.Table.select`.
        named_values
            Like values, but with names, just like `ibis.Table.select`.

        Examples
        --------
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"record_id": [1, 2, 3], "address": ["a", "b", "c"]})
        >>> right = ibis.memtable({"record_id": [8, 9], "address": ["x", "y"]})
        >>> links_raw = ibis.memtable({"record_id_l": [1, 3], "record_id_r": [8, 9]})
        >>> links = LinksTable(links_raw, left=left, right=right)
        >>> links.with_left(
        ...     "address",
        ...     ibis._.address.upper().name("address_upper"),
        ...     left_address=ibis._.address,
        ... )
        ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
        ┃ record_id_l ┃ record_id_r ┃ address ┃ address_upper ┃ left_address ┃
        ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
        │ int64       │ int64       │ string  │ string        │ string       │
        ├─────────────┼─────────────┼─────────┼───────────────┼──────────────┤
        │           1 │           8 │ a       │ A             │ a            │
        │           3 │           9 │ c       │ C             │ c            │
        └─────────────┴─────────────┴─────────┴───────────────┴──────────────┘
        """
        if not values and not named_values:
            values = self.left.columns

        uname = _util.unique_name()
        left = self.left.select(_.record_id.name(uname), *values, **named_values)
        conflicts = [c for c in left.columns if c in self.columns]
        if conflicts:
            raise ValueError(f"conflicting columns: {conflicts}")
        joined = self.left_join(left, self.record_id_l == left[uname]).drop(uname)
        return LinksTable(joined, left=self._left_raw, right=self._right_raw)

    def with_right(
        self,
        *values: ibis.Deferred | Callable[[ibis.Table], ibis.Value] | None,
        **named_values: ibis.Deferred | Callable[[ibis.Table], ibis.Value] | None,
    ) -> LinksTable:
        """Add columns from the right table to this table of links.

        This allows you to add specific columns from the right table,
        renaming or modifying them as needed, following the `ibis.Table` pattern of
        `my_table.select("my_col", new_col=my_table.foo)`,
        except here we choose from the right table.

        Parameters
        ----------
        values
            The columns to add from the right table.
            Support string names, Deferreds, etc, just like `ibis.Table.select`.
        named_values
            Like values, but with names, just like `ibis.Table.select`.

        Examples
        --------
        >>> ibis.options.interactive = True
        >>> left = ibis.memtable({"record_id": [1, 2, 3], "address": ["a", "b", "c"]})
        >>> right = ibis.memtable({"record_id": [8, 9], "address": ["x", "y"]})
        >>> links_raw = ibis.memtable({"record_id_l": [1, 3], "record_id_r": [8, 9]})
        >>> links = LinksTable(links_raw, left=left, right=right)
        >>> links.with_right(
        ...     "address",
        ...     ibis._.address.upper().name("address_upper"),
        ...     right_address=ibis._.address,
        ... )
        ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
        ┃ record_id_l ┃ record_id_r ┃ address ┃ address_upper ┃ right_address ┃
        ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
        │ int64       │ int64       │ string  │ string        │ string        │
        ├─────────────┼─────────────┼─────────┼───────────────┼───────────────┤
        │           1 │           8 │ x       │ X             │ x             │
        │           3 │           9 │ y       │ Y             │ y             │
        └─────────────┴─────────────┴─────────┴───────────────┴───────────────┘
        """
        if not values and not named_values:
            values = self.right.columns

        uname = _util.unique_name()
        right = self.right.select(_.record_id.name(uname), *values, **named_values)
        conflicts = [c for c in right.columns if c in self.columns]
        if conflicts:
            raise ValueError(f"conflicting columns: {conflicts}")
        joined = self.left_join(right, self.record_id_r == right[uname]).drop(uname)
        return LinksTable(joined, left=self._left_raw, right=self._right_raw)

    def with_both(self) -> LinksTable:
        """
        Add all columns from `left` and `right` with suffixes `_l` and `_r`
        """
        left_columns = [
            _[c].name(c + "_l")
            for c in self.left.columns
            if c + "_l" not in self.columns
        ]
        right_columns = [
            _[c].name(c + "_r")
            for c in self.right.columns
            if c + "_r" not in self.columns
        ]
        x = self
        x = x.with_left(*left_columns)
        x = x.with_right(*right_columns)
        return x

    @property
    def left(self) -> LinkedTable:
        """The left table."""
        from mismo.types._linked_table import LinkedTable

        return LinkedTable(self._left_raw, other=self._right_raw, links=self)

    @property
    def right(self) -> LinkedTable:
        """The right table."""
        from mismo.types._linked_table import LinkedTable

        return LinkedTable(
            self._right_raw, other=self._left_raw, links=self._swap_perspective(self)
        )

    @staticmethod
    def _swap_perspective(links: ibis.Table) -> ibis.Table:
        def _swap_l_and_r(name: str):
            if name.endswith("_l"):
                return name[:-2] + "_r"
            if name.endswith("_r"):
                return name[:-2] + "_l"
            return name

        return links.rename(_swap_l_and_r)

    def cache(self) -> LinksTable:
        """Cache the links table."""
        return LinksTable(self._t.cache(), left=self._left_raw, right=self._right_raw)
