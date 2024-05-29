from __future__ import annotations

from typing import Protocol, runtime_checkable

from ibis.expr import types as ir


@runtime_checkable
class PComparer(Protocol):
    """A Callable that adds column(s) of features to a table of record pairs."""

    def __call__(self, pairs: ir.Table, **kwargs) -> ir.Table:
        """Add column(s) of features to a table of record pairs.

        For example, add a match score to each record pair, modify a score from
        a previous PComparer, or similar.

        Implementers *must* expect to be called with a table of record pairs.
        Columns suffixed with "_l" come from the left table, columns suffixed
        with "_r" come from the right table, and columns with neither suffix
        are features of the pair itself (eg from a different PComparer).
        """
