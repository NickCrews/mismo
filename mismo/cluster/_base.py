from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple, Protocol

from ibis.expr.types import Table

from mismo.compare._base import PComparisons


@dataclass(frozen=True, kw_only=True)
class Labeling:
    """Holds labeling information for a dataset."""

    table: Table
    labels: Table
    """A table with the columns (record_id, label), where label is a uint64."""

    def with_labels(self, col_name: str = "label") -> Table:
        """The dataset table with the "labels" column added."""
        if col_name in self.table.columns:
            raise ValueError(f"Dataset already has a column named '{col_name}'")
        labels = self.labels.relabel({"label": col_name})
        return self.table.inner_join(labels, "record_id")


class LabelingPair(NamedTuple):
    """Holds a pair of Labeling"""

    left: Labeling
    right: Labeling


class PClusterer(Protocol):
    """Takes pairwise comparisons and clusters the records into groups.

    You can think of this as a graph algorithm, where the nodes are the records,
    and the edges are the comparisons. The algorithm determines the "clusters"
    of records within the graph and assigns a cluster ID to each record.
    """

    def cluster(self, comparisons: PComparisons) -> LabelingPair:
        ...


def check_labels(t: Table) -> Table:
    if t.columns != ["record_id", "label"]:
        raise ValueError(f"Expected columns ['record_id', 'label'], got {t.columns}")
    if not t.label.type().is_unsigned_integer():
        raise ValueError(
            f"Expected label column to be unsigned integer, got {t.label.type()}"
        )
