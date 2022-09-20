"""Scores the pairwise similarity of records between two sets to records."""
from __future__ import annotations

from typing import Iterable

import pyarrow as pa
from vaex.dataframe import DataFrame

from mismo._typing import Protocol
from mismo.block._blocker import PBlocking


class PScorer(Protocol):
    def score(
        self, datal: DataFrame, datar: DataFrame, blocking: PBlocking
    ) -> PScoring:
        """Score the pairwise similarity of records between two sets to records.

        Args:
            datal: The left set of records.
            datar: The right set of records.
            blocking: The blocking result from PBlocker.block(), which tells us which
                pairs of records to compare.

        Returns:
            A copy of ``links`` with a "score" column added, where the score is the
            probability that the two records are a match. e.g. 0.0 means definitely
            not a match, 1.0 means definitely a match, and 0.5 means we're not sure.
        """
        ...


class PScoring(Protocol):
    """The result of running Scorer.score().

    Since the result might be larger than memory, we might not want to materialize the
    whole thing. So this class is a wrapper around the result, which can be used to
    materialize the result, or to iterate over it.
    """

    def __len__(self) -> int:
        """Return the number of scores."""

    def to_arrow(self) -> pa.Table:
        """Materialize the result as an Arrow table."""
        ...

    def iter_arrow(self, chunk_size: int) -> Iterable[pa.Table]:
        """Iterate over the result as an iterator of Arrow tables."""
        ...


def make_pairs(datal: DataFrame, datar: DataFrame, links: DataFrame) -> DataFrame:
    """
    Create a DataFrame of pairs of indices from the given links.
    """
    pairs = links.copy()
    pairs["index_left"] = pairs["index_left"].map(datal.index)
    pairs["index_right"] = pairs["index_right"].map(datar.index)
    return pairs
