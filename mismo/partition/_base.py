from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple, Protocol

from ibis.expr.types import Table

from mismo.compare._base import PComparisons


@dataclass(frozen=True, kw_only=True)
class Partitioning:
    """Holds partitioning information on a Dataset."""

    table: Table
    labels: Table
    """A table with the columns (record_id, label), where label is a uint64."""

    def with_labels(self, col_name: str = "label") -> Table:
        """The dataset table with the "labels" column added."""
        if col_name in self.table.columns:
            raise ValueError(f"Dataset already has a column named '{col_name}'")
        labels = self.labels.relabel({"label": col_name})
        return self.table.inner_join(labels, "record_id")


class PartitioningPair(NamedTuple):
    """Holds partitioning information on a DatasetPair."""

    left: Partitioning
    right: Partitioning


class PPartitioner(Protocol):
    """Takes a set if pairwise comparisons and partitions the records into groups.

    You can think of this as a graph algorithm, where the nodes are the records,
    and the edges are the comparisons. The algorithm determines the "clusters"
    of records within the graph and assigns a cluster ID to each record.
    """

    def partition(self, comparisons: PComparisons) -> PartitioningPair:
        ...


def check_labels(t: Table) -> Table:
    if t.columns != ["record_id", "label"]:
        raise ValueError(f"Expected columns ['record_id', 'label'], got {t.columns}")
    if not t.label.type().is_unsigned_integer():
        raise ValueError(
            f"Expected label column to be unsigned integer, got {t.label.type()}"
        )
