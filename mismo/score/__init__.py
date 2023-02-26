"""Scores the pairwise similarity of records between two sets to records."""
from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table


class PScorer(Protocol):
    def score(self, blocked: Table) -> Table:
        """Score the pairwise similarity of records between two sets of records.

        Args:
            blocked: The blocked pairs of records.

        Returns:
            A copy of ``blocked`` with a "score" column added, where the score is the
            probability that the two records are a match. e.g. 0.0 means definitely
            not a match, 1.0 means definitely a match, and 0.5 means we're not sure.
        """
