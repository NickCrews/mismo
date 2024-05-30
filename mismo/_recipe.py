from __future__ import annotations

from typing import Protocol

from ibis.expr import types as ir


class PRecipe(Protocol):
    """Preps, blocks, and compares records."""

    def prepare(self, t: ir.Table) -> ir.Table:
        """Prepare the data for blocking and comparison."""

    def block(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        """Block tables into record pairs."""

    def compare(self, t: ir.Table) -> ir.Table:
        """Perform comparisons on the blocked pairs."""
