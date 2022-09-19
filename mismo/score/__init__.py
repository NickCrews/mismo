"""Scores the pairwise similarity of records between two sets to records."""

from vaex.dataframe import DataFrame

from mismo._typing import Protocol


class PScorer(Protocol):
    def score(self, datal: DataFrame, datar: DataFrame, links: DataFrame) -> DataFrame:
        """Score the pairwise similarity of records between two sets to records.

        Args:
            datal: The left set of records.
            datar: The right set of records.
            links: A DataFrame of edges between the records in datal and datar.
                   Contains columns "lid" and "rid" that are the indices of the records.

        Returns:
            A copy of ``links`` with a "score" column added, where the score is the
            probability that the two records are a match. e.g. 0.0 means definitely
            not a match, 1.0 means definitely a match, and 0.5 means we're not sure.
        """
        ...


def make_pairs(datal: DataFrame, datar: DataFrame, links: DataFrame) -> DataFrame:
    """
    Create a DataFrame of pairs of indices from the given links.
    """
    pairs = links.copy()
    pairs["index_left"] = pairs["index_left"].map(datal.index)
    pairs["index_right"] = pairs["index_right"].map(datar.index)
    return pairs
