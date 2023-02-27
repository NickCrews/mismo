"""Compare record pairs between two datasets."""
from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table

from mismo.block._blocker import PBlocking


class PComparer(Protocol):
    def compare(self, blocking: PBlocking) -> Table:
        """Compare two datasets, adding scores and/or other features to each pair.

        Args:
            blocking: The blocked pairs of records.

        Returns:
            A copy of ``blocking`` with a "score" column added, where the score is the
            probability that the two records are a match. e.g. 0.0 means definitely
            not a match, 1.0 means definitely a match, and 0.5 means we're not sure.
        """
