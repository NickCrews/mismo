"""Compare record pairs between two datasets."""
from __future__ import annotations

import dataclasses
from typing import Protocol, runtime_checkable

from ibis.expr.types import Table

from mismo._dataset import PDatasetPair
from mismo._typing import Self
from mismo.block import PBlocking


@runtime_checkable
class PComparer(Protocol):
    def compare(self, blocking: PBlocking) -> PComparisons:
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
    def blocking(self) -> PBlocking:
        """The Blocking that was used to generate the comparisons"""
        ...

    @property
    def dataset_pair(self) -> PDatasetPair:
        """The DatasetPair that was compared."""
        return self.blocking.dataset_pair

    @property
    def compared(self) -> Table:
        """A table of (left_id, right_id, score) triples"""
        ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class Comparisons:
    blocking: PBlocking
    compared: Table

    @property
    def dataset_pair(self) -> PDatasetPair:
        return self.blocking.dataset_pair

    def cache(self) -> Self:
        return dataclasses.replace(self, compared=self.compared.cache())
