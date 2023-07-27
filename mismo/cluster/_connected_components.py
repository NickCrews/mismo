from __future__ import annotations

from itertools import count
import logging
from typing import Iterable

import ibis
from ibis import _
from ibis.expr.types import Column, IntegerColumn, Table

from mismo import _util

logger = logging.getLogger(__name__)


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

    Parameters
    ----------
    edges:
        A table with the columns (record_id_l, record_id_r).
        The datatypes can be anything.

    Returns
    -------
        Two tables, one for the left records and one for the right records.
        The left table has columns (record_id_l, component), and
        the right table has columns (record_id_r, component).
    """
    edges, left_map, right_map = _normalize_edges(edges)
    labels = _connected_components_ints(edges, max_iter=max_iter)

    def _join(left, right):
        return _util.join(
            left, right, "record", how="left", lname="{name}_l", rname="{name}_r"
        ).drop("record_l", "record_r")

    left_labels = _join(left_map, labels)
    right_labels = _join(right_map, labels)
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
    condition = (labels.record == new_labels.record) & (
        labels.component != new_labels.component
    )
    return _util.join(labels, new_labels, condition).count().execute()


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
    """
    Translate edges to uint64s, merge namespaces, and create maps back to the original

    By "merge namespaces" I mean the record ids
    in the left table and the record ids in the right table are not necessarily
    distinct from each other. They could each have an id of 5, but refer to
    different records. Keeping track of these two different universes of record
    ids is complex, so we just merge them into one universe of record ids,
    by labeling the left records 0-L, and then the right records starting
    from there, eg  L+1-L+R.
    """
    if "record_id_l" not in raw_edges.columns or "record_id_r" not in raw_edges.columns:
        raise ValueError(
            "edges must contain the columns `record_id_l` and `record_id_r`, "
            f"but it contains {raw_edges.columns}"
        )
    left_map = raw_edges.select(
        "record_id_l", record=_group_id(raw_edges, "record_id_l")
    ).distinct()
    right_map = raw_edges.select(
        "record_id_r", record=_group_id(raw_edges, "record_id_r")
    ).distinct()
    max_left = left_map.count().execute()
    right_map = right_map.mutate(
        record=(right_map["record"] + max_left + 1).cast("uint64")
    )

    lm2 = left_map.relabel({"record": "record_l"})
    rm2 = right_map.relabel({"record": "record_r"})
    edges = raw_edges.left_join(lm2, "record_id_l").left_join(rm2, "record_id_r")
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
