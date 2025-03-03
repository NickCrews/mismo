from __future__ import annotations

from typing import TYPE_CHECKING

import ibis

from mismo import _typing
from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    from mismo.types._linked_table import LinkedTable


class LinksTable(TableWrapper):
    """A table of links between two tables."""

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

    def with_left(self) -> _typing.Self:
        """Join the left table to this table of links."""
        left_renamed = self.left_.rename("{name}_l")
        joined = self.inner_join(left_renamed, "record_id_l")
        return self.__class__(joined, left=self._left_raw, right=self._right_raw)

    def with_right(self) -> _typing.Self:
        """Join the right table to this table of links."""
        right_renamed = self.right_.rename("{name}_r")
        joined = self.inner_join(right_renamed, "record_id_r")
        return self.__class__(joined, left=self._left_raw, right=self._right_raw)

    @property
    def left_(self) -> LinkedTable:
        """The left table."""
        from mismo.types._linked_table import LinkedTable

        return LinkedTable(self._left_raw, other=self._right_raw, links=self)

    @property
    def right_(self) -> LinkedTable:
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
