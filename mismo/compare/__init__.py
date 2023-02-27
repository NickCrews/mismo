"""Compare record pairs between two datasets."""
from __future__ import annotations

import dataclasses
from typing import Protocol

from ibis.expr.types import Table

from mismo._dataset import PDatasetPair
from mismo.block._blocker import PBlocking


class PComparer(Protocol):
    def compare(self, blocking: PBlocking) -> PComparisons:
        """Compare two datasets, adding scores and/or other features to each pair.

        Args:
            blocking: The blocked pairs of records.

        Returns:
            PComparisons
        """
        ...


class PComparisons(Protocol):
    """Record pairs, with scores and/or other features added to each pair."""

    @property
    def dataset_pair(self) -> PDatasetPair:
        """The DatasetPair that was compared."""
        return self.blocking.dataset_pair

    @property
    def blocking(self) -> PBlocking:
        """The Blocking that was used to generate the comparisons"""
        ...

    @property
    def compared(self) -> Table:
        """A table of (left_id, right_id, score) triples"""
        ...


@dataclasses.dataclass(frozen=True)
class Comparisons(PComparisons):
    blocking: PBlocking
    compared: Table
