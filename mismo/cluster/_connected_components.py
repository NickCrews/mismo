from __future__ import annotations

from itertools import count
import logging
from typing import Callable

import ibis
from ibis import _
from ibis.expr.types import Table

from mismo import _util
from mismo._factorizer import Factorizer

logger = logging.getLogger(__name__)


def connected_components(edges: Table, max_iter: int | None = None) -> Table:
    """Compute the connected components of a graph.

    This is based on the algorithm described at
    https://www.drmaciver.com/2008/11/computing-connected-graph-components-via-sql/
    This is linear in terms of the size of a component. This is usually
    acceptable for our use case, because we expect the components to be small.
    I think more performant algorithms exist, but they are more complicated.
    See https://arxiv.org/pdf/1802.09478.pdf

    Parameters
    ----------
    edges :
        A table with the columns (record_id_l, record_id_r).
        The datatypes can be anything, but they must be the same.
    max_iter : int, optional
        The maximum number of iterations to run. If None, run until convergence.

    Returns
    -------
    labels : Table
        Labeling for left records. Has columns (record_id, component: uint64).

    !!! note

        The record_ids are assumed to refer to the same universe of records:
        If record_id 5 appears in both the left column and the right column,
        then they are assumed to refer to the same record. If you have a left
        dataset and right dataset and they both have a record with id 5, then
        you should first make the ids unique across the two datasets.

    Examples
    --------
    >>> from mismo.cluster import connected_components
    >>> edges_list = [
    ...     ("a", "x"),
    ...     ("b", "x"),
    ...     ("b", "y"),
    ...     ("c", "y"),
    ...     ("c", "z"),
    ...     ("g", "h"),
    ... ]
    >>> edges_df = pd.DataFrame(edges_list, columns=["record_id_l", "record_id_r"])
    >>> edges = ibis.memtable(edges_df)
    >>> connected_components(edges)
    ┏━━━━━━━━━━━┳━━━━━━━━━━━┓
    ┃ record_id ┃ component ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━┩
    │ string    │ uint64    │
    ├───────────┼───────────┤
    │ x         │         0 │
    │ y         │         0 │
    │ z         │         0 │
    │ h         │         3 │
    │ a         │         0 │
    │ b         │         0 │
    │ c         │         0 │
    │ g         │         3 │
    └───────────┴───────────┘
    """
    int_edges, restore = _intify_edges(edges)
    int_labels = _connected_components_ints(int_edges, max_iter=max_iter)
    return restore(int_labels)


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
    condition = (labels.record_id == new_labels.record_id) & (
        labels.component != new_labels.component
    )
    return _util.join(labels, new_labels, condition).count().execute()


def _updated_labels(node_labels: Table, edges: Table) -> Table:
    component_equivalences = _get_component_equivalences(edges, node_labels)
    component_mapping = _get_component_update_map(component_equivalences)
    return node_labels.relabel({"component": "component_old"}).left_join(
        component_mapping, "component_old"
    )["record_id", "component"]


def _get_component_equivalences(edges: Table, node_labels: Table) -> Table:
    """Get a table of which components are equivalent to each other."""
    same_components = (
        edges.join(node_labels, edges.record_id_l == node_labels.record_id)
        .relabel({"component": "component_l"})
        .drop("record_id", "record_id_l")
    )
    same_components = (
        same_components.join(
            node_labels, same_components.record_id_r == node_labels.record_id
        )
        .relabel({"component": "component_r"})
        .drop("record_id", "record_id_r")
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


def _intify_edges(raw_edges: Table) -> tuple[Table, Callable[[Table], Table]]:
    """Translate edges to uint64s and create restoring function"""
    if "record_id_l" not in raw_edges.columns or "record_id_r" not in raw_edges.columns:
        raise ValueError(
            "edges must contain the columns `record_id_l` and `record_id_r`, "
            f"but it contains {raw_edges.columns}"
        )

    swapped = raw_edges.relabel(
        {"record_id_l": "record_id_r", "record_id_r": "record_id_l"}
    )
    all_node_ids = raw_edges.union(swapped).select("record_id_l")

    f = Factorizer(all_node_ids, "record_id_l")
    edges = f.encode(raw_edges, "record_id_l")
    edges = f.encode(edges, "record_id_r")

    def restore(int_labels: Table) -> Table:
        return f.decode(int_labels, "record_id")

    return edges, restore


def _get_initial_labels(edges: Table) -> Table:
    labels_left = edges.select(record_id=_.record_id_l, component=_.record_id_l)
    labels_right = edges.select(record_id=_.record_id_r, component=_.record_id_r)
    return ibis.union(labels_left, labels_right, distinct=True)
