from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple, Protocol

from ibis.expr.types import Table

from mismo._dataset import Dataset
from mismo.compare import PComparisons


@dataclass(frozen=True, kw_only=True)
class Partitioning:
    """Holds partitioning information on a Dataset."""

    dataset: Dataset
    labels: Table
    """A table of (record_id, cluster_id) pairs."""

    def with_labels(self) -> Table:
        """The dataset table with the cluster labels added."""
        return self.dataset.table.inner_join(self.labels, self.dataset.record_id_column)


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
