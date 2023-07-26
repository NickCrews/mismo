"""Compare record pairs between two datasets."""
from __future__ import annotations

import dataclasses
from typing import Protocol, runtime_checkable

from ibis.expr.types import Table

from mismo._typing import Self
from mismo.block import Blocking


@runtime_checkable
class PComparer(Protocol):
    def compare(self, blocking: Blocking) -> PComparisons:
        """Compare two datasets, adding scores and/or other features to each pair.

        Args:
            blocking: The blocked pairs of records.

        Returns:
            PComparisons
        """
        ...


@runtime_checkable
class PComparisons(Protocol):
    """Record pairs, with scores and/or other features added to each pair."""

    @property
    def blocking(self) -> Blocking:
        """The Blocking that was used to generate the comparisons"""
        ...

    @property
    def left(self) -> Table:
        """The left source Table"""
        ...

    @property
    def right(self) -> Table:
        """The right source Table"""
        ...

    @property
    def compared(self) -> Table:
        """A table of (left_id, right_id, score) triples"""
        ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class Comparisons:
    blocking: Blocking
    compared: Table

    @property
    def left(self) -> Table:
        return self.blocking.left

    @property
    def right(self) -> Table:
        return self.blocking.right

    def cache(self) -> Self:
        return dataclasses.replace(self, compared=self.compared.cache())
