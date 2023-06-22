from __future__ import annotations

from itertools import count
import logging
from typing import Iterable

import ibis
from ibis import _
from ibis.expr.types import Column, IntegerColumn, Table

from mismo.compare._base import PComparisons

from . import Partitioning, PartitioningPair, PPartitioner

logger = logging.getLogger(__name__)


class ConnectedComponentsPartitioner(PPartitioner):
    """Uses a connected components algorithm to partition records into groups."""

    def __init__(
        self, min_bayes_factor: float | None = None, max_iter: int | None = None
    ):
        self.min_bayes_factor = min_bayes_factor
        self.max_iter = max_iter

    def partition(self, comparisons: PComparisons) -> PartitioningPair:
        raw_edges = comparisons.compared
        if self.min_bayes_factor is not None:
            raw_edges = raw_edges[_.bayes_factor > self.min_bayes_factor]
        raw_edges = raw_edges["record_id_l", "record_id_r"]
        left_labels, right_labels = connected_components(
            raw_edges, max_iter=self.max_iter
        )
        left_table = comparisons.dataset_pair.left
        right_table = comparisons.dataset_pair.right
        left_partitioning = Partitioning(
            table=left_table,
            labels=left_labels.relabel(
                {"record_id_l": "record_id", "component": "label"}
            ),
        )
        right_partitioning = Partitioning(
            table=right_table,
            labels=right_labels.relabel(
                {"record_id_r": "record_id", "component": "label"}
            ),
        )
        return PartitioningPair(left_partitioning, right_partitioning)


def connected_components(
    edges: Table, max_iter: int | None = None
) -> tuple[Table, Table]:
    """Compute the connected components of a graph.

    This is based on the algorithm described at
    https://www.drmaciver.com/2008/11/computing-connected-graph-components-via-sql/
    This is linear in terms of the size of a component. This is usually
    acceptable for our use case, because we expect the components to be small.
    I think more performant algorithms exist, but they are more complicated.
    See https://arxiv.org/pdf/1802.09478.pdf

    Args:
        edges: A table of (left_id, right_id) pairs. This must have two columns,
        they can be named anything. The datatypes can be anything.

    Returns:
        Two tables, one for the left ids and one for the right ids. Each table
        has two columns, the first is the id of the record and the second is
        called "component" and is the id of the component it belongs to.
    """
    edges, left_map, right_map = _normalize_edges(edges)
    labels = _connected_components_ints(edges, max_iter=max_iter)
    left_labels = left_map.left_join(labels, "record").drop("record_x", "record_y")
    right_labels = right_map.left_join(labels, "record").drop("record_x", "record_y")
    return left_labels, right_labels


def _connected_components_ints(
    edges: Table, max_iter: int | None = None
) -> tuple[Table, Table]:
    """The core algorithm. Assumes you already translated the record ids to ints."""
    labels = _get_initial_labels(edges).cache()
    for i in count(1):
        new_labels = _updated_labels(labels, edges).cache()
        assert (labels.count() == new_labels.count()).execute()
        n_updates = _n_updates(labels, new_labels)
        if n_updates == 0:
            return labels
        logger.info(f"Round {i}: Updated {n_updates} labels")
        labels = new_labels
        if max_iter is not None and i >= max_iter:
            return labels


def _n_updates(labels: Table, new_labels: Table) -> int:
    """Count the number of updates between two labelings."""
    return (
        labels.join(new_labels, "record")
        .filter(_.component_x != _.component_y)
        .count()
        .execute()
    )


def _updated_labels(labels: Table, edges: Table) -> Table:
    component_equivalences = _get_component_equivalences(edges, labels)
    component_mapping = _get_component_update_map(component_equivalences)
    return labels.relabel({"component": "component_old"}).left_join(
        component_mapping, "component_old"
    )["record", "component"]


def _get_component_equivalences(edges: Table, labels: Table) -> Table:
    """Get a table of which components are equivalent to each other."""
    same_components = (
        edges.join(labels, edges["record_l"] == labels["record"])
        .relabel({"component": "component_l"})
        .drop("record", "record_l")
    )
    same_components = (
        same_components.join(labels, same_components["record_r"] == labels["record"])
        .relabel({"component": "component_r"})
        .drop("record", "record_r")
    )
    return same_components["component_l", "component_r"].distinct()


def _get_component_update_map(component_equivalences: Table) -> Table:
    """Create mapping from old component ids to new component ids"""
    representative = ibis.least(
        component_equivalences.component_l, component_equivalences.component_r
    )
    m = component_equivalences.mutate(component=representative)
    ml = m.relabel({"component_l": "component_old"})["component_old", "component"]
    mr = m.relabel({"component_r": "component_old"})["component_old", "component"]
    together = ibis.union(ml, mr, distinct=True)
    together = together.group_by(_.component_old).agg(component=_.component.min())
    return together["component_old", "component"]


def _normalize_edges(raw_edges: Table) -> tuple[Table, Table, Table]:
    """Translate the edges to use uint64s, and create maps back to the original ids."""
    if len(raw_edges.columns) != 2:
        raise ValueError(
            f"edges must have exactly two columns, got {raw_edges.columns}"
        )
    cl, cr = raw_edges.columns
    left_map = raw_edges.select(cl, record=_group_id(raw_edges, cl)).distinct()
    right_map = raw_edges.select(cr, record=_group_id(raw_edges, cr)).distinct()
    max_left = left_map.count().execute()
    right_map = right_map.mutate(
        record=(right_map["record"] + max_left + 1).cast("uint64")
    )

    lm2 = left_map.relabel({"record": "record_l"})
    rm2 = right_map.relabel({"record": "record_r"})
    edges = raw_edges.left_join(lm2, cl).left_join(rm2, cr)
    edges = edges["record_l", "record_r"]
    return edges, left_map, right_map


def _get_initial_labels(edges: Table) -> Table:
    labels_left = edges.select(record=_["record_l"], component=_["record_l"])
    labels_right = edges.select(record=_["record_r"], component=_["record_r"])
    return ibis.union(labels_left, labels_right, distinct=True)


def _group_id(t: Table, keys: str | Column | Iterable[str | Column]) -> IntegerColumn:
    """Number each group from 0 to the "number of groups - 1".

    This is equivalent to pandas.DataFrame.groupby(keys).ngroup().
    """
    # We need an arbitrary column to use for dense_rank
    # https://github.com/ibis-project/ibis/issues/5408
    col: Column = t[t.columns[0]]
    return col.dense_rank().over(ibis.window(order_by=keys)).cast("uint64")
