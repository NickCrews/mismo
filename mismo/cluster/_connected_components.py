from __future__ import annotations

from itertools import count
import logging
from typing import Callable, Iterable

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo._datasets import Datasets
from mismo._factorizer import Factorizer

logger = logging.getLogger(__name__)


# I think more performant algorithms exist, but they are more complicated.
# See https://arxiv.org/pdf/1802.09478.pdf
def connected_components(
    *,
    records: ir.Table | Iterable[ir.Table] = None,
    links: ir.Table,
    max_iter: int | None = None,
) -> Datasets:
    """
    Add a `component` column using connected components, based on the given links.

    This uses [an iterative algorithm](https://www.drmaciver.com/2008/11/computing-connected-graph-components-via-sql/)
    that is linear in terms of the diameter of the largest component.
    This is usually acceptable for our use case,
    because we expect the components to be small.

    Parameters
    ----------
    records :
        A Table with the column record_id, or an iterable of these.

        !!! note

            If you supply multiple Tables, the record_ids must be the same type
            across all tables, and **universally** unique across all tables

    links :
        A table with the columns (record_id_l, record_id_r), corresponding
        to the `record_id`s in `records`.
    max_iter :
        The maximum number of iterations to run. If None, run until convergence.

    Returns
    -------
    labeled :
        If `records` is a single Table, a single Table will be returned
        with a `component:uint64` column added.
        If an iterable is given, a `Datasets` will be returned, with
        a `component:uint64` column added to each contained Table.

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> from mismo.cluster import connected_components
    >>> records1 = ibis.memtable(
    ...     [
    ...         ("a", 0),
    ...         ("b", 1),
    ...         ("c", 2),
    ...         ("d", 3),
    ...         ("g", 6),
    ...     ],
    ...     columns=["record_id", "other"]
    ... )
    >>> records2 = ibis.memtable(
    ...     [
    ...         ("h", 7),
    ...         ("x", 23),
    ...         ("y", 24),
    ...         ("z", 25),
    ...     ],
    ...     columns=["record_id", "other"]
    ... )
    >>> links = ibis.memtable(
    ...     [
    ...         ("a", "x"),
    ...         ("b", "x"),
    ...         ("b", "y"),
    ...         ("c", "y"),
    ...         ("c", "z"),
    ...         ("g", "h"),
    ...     ],
    ...     columns=["record_id_l", "record_id_r"])

    If you don't supply the records, then you just get a labeling map
    from record_id -> component. Note how only the record_ids that are
    present in `links` are returned, eg there is no record_id `"d"` present:

    >>> connected_components(links=links)
    ┏━━━━━━━━━━━┳━━━━━━━━━━━┓
    ┃ record_id ┃ component ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━┩
    │ string    │ uint64    │
    ├───────────┼───────────┤
    │ b         │         0 │
    │ a         │         0 │
    │ c         │         0 │
    │ y         │         0 │
    │ x         │         0 │
    │ z         │         0 │
    │ g         │         3 │
    │ h         │         3 │
    └───────────┴───────────┘

    If you supply records, then the records are labeled with the component:

    >>> connected_components(records=records1, links=links)
    ┏━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
    ┃ record_id ┃ other ┃ component ┃
    ┡━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
    │ string    │ int64 │ uint64    │
    ├───────────┼───────┼───────────┤
    │ b         │     1 │         0 │
    │ a         │     0 │         0 │
    │ c         │     2 │         0 │
    │ g         │     6 │         3 │
    │ d         │     3 │         4 │
    └───────────┴───────┴───────────┘

    You can supply multiple sets of records, which are coerced to a `Datasets`,
    and returned as a `Datasets`, with each table of records labeled
    individually.

    >>> a, b = connected_components(records=(records1, records2), links=links)
    >>> a
    ┏━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
    ┃ record_id ┃ other ┃ component ┃
    ┡━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
    │ string    │ int64 │ uint64    │
    ├───────────┼───────┼───────────┤
    │ g         │     6 │         3 │
    │ c         │     2 │         0 │
    │ b         │     1 │         0 │
    │ a         │     0 │         0 │
    │ d         │     3 │         4 │
    └───────────┴───────┴───────────┘
    >>> b
    ┏━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┓
    ┃ record_id ┃ other ┃ component ┃
    ┡━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━┩
    │ string    │ int64 │ uint64    │
    ├───────────┼───────┼───────────┤
    │ x         │    23 │         0 │
    │ h         │     7 │         3 │
    │ y         │    24 │         0 │
    │ z         │    25 │         0 │
    └───────────┴───────┴───────────┘
    """  # noqa: E501
    int_edges, restore = _intify_edges(links)
    int_labels = _connected_components_ints(int_edges, max_iter=max_iter)
    labels = restore(int_labels)
    if records is None:
        return labels
    ds = Datasets(records)
    result = _label_datasets(ds, labels)
    if isinstance(records, ir.Table):
        return result[0]
    return result


def _connected_components_ints(
    edges: ir.Table, max_iter: int | None = None
) -> tuple[ir.Table, ir.Table]:
    """The core algorithm. Assumes you already translated the record ids to ints."""
    labels = _get_initial_labels(edges).cache()
    for i in count(1):
        new_labels = _updated_labels(labels, edges).cache()
        assert labels.count().execute() == new_labels.count().execute()
        n_updates = _n_updates(labels, new_labels)
        if n_updates == 0:
            return labels
        logger.info(f"Round {i}: Updated {n_updates} labels")
        labels = new_labels
        if max_iter is not None and i >= max_iter:
            return labels


def _n_updates(labels: ir.Table, new_labels: ir.Table) -> int:
    """Count the number of updates between two labelings."""
    condition = (labels.record_id == new_labels.record_id) & (
        labels.component != new_labels.component
    )
    return ibis.join(labels, new_labels, condition).count().execute()


def _updated_labels(node_labels: ir.Table, edges: ir.Table) -> ir.Table:
    component_equivalences = _get_component_equivalences(edges, node_labels)
    component_mapping = _get_component_update_map(component_equivalences)
    return node_labels.rename(component_old="component").left_join(
        component_mapping, "component_old"
    )["record_id", "component"]


def _get_component_equivalences(edges: ir.Table, node_labels: ir.Table) -> ir.Table:
    """Get a table of which components are equivalent to each other."""
    same_components = (
        edges.join(node_labels, edges.record_id_l == node_labels.record_id)
        .rename(component_l="component")
        .drop("record_id", "record_id_l")
    )
    same_components = (
        same_components.join(
            node_labels, same_components.record_id_r == node_labels.record_id
        )
        .rename(component_r="component")
        .drop("record_id", "record_id_r")
    )
    return same_components["component_l", "component_r"].distinct()


def _get_component_update_map(component_equivalences: ir.Table) -> ir.Table:
    """Create mapping from old component ids to new component ids"""
    representative = ibis.least(
        component_equivalences.component_l, component_equivalences.component_r
    )
    m = component_equivalences.mutate(component=representative)
    ml = m.select("component", component_old="component_l")
    mr = m.select("component", component_old="component_r")
    together = ibis.union(ml, mr, distinct=True)
    together = together.group_by(_.component_old).agg(component=_.component.min())
    return together["component_old", "component"]


def _intify_edges(
    raw_edges: ir.Table,
) -> tuple[ir.Table, Callable[[ir.Table], ir.Table]]:
    """Translate edges to uint64s and create restoring function"""
    if "record_id_l" not in raw_edges.columns or "record_id_r" not in raw_edges.columns:
        raise ValueError(
            "edges must contain the columns `record_id_l` and `record_id_r`, "
            f"but it contains {raw_edges.columns}"
        )
    raw_edges = raw_edges.select("record_id_l", "record_id_r")
    swapped = raw_edges.select(record_id_r=_.record_id_l, record_id_l=_.record_id_r)
    all_node_ids = raw_edges.union(swapped).select("record_id_l")

    f = Factorizer(all_node_ids, "record_id_l")
    edges = f.encode(raw_edges, "record_id_l")
    edges = f.encode(edges, "record_id_r")

    def restore(int_labels: ir.Table) -> ir.Table:
        return f.decode(int_labels, "record_id")

    return edges, restore


def _get_initial_labels(edges: ir.Table) -> ir.Table:
    labels_left = edges.select(record_id=_.record_id_l, component=_.record_id_l)
    labels_right = edges.select(record_id=_.record_id_r, component=_.record_id_r)
    return ibis.union(labels_left, labels_right, distinct=True)


def _label_datasets(ds: Datasets, labels: ir.Table) -> Datasets:
    additional_labels = _get_additional_labels(labels, ds.all_record_ids())
    labels = labels.union(additional_labels)
    return ds.map(
        lambda name, t: t.left_join(labels, "record_id", rname="{name}_ibis_tmp").drop(
            "record_id_ibis_tmp"
        )
    )


def _get_additional_labels(labels: ir.Table, record_ids: ir.Column) -> ir.Table:
    nodes = record_ids.name("record_id").as_table()
    is_missing_label = nodes.record_id.notin(labels.record_id)
    max_existing_label = labels.component.max()
    additional_labels = nodes[is_missing_label].select(
        "record_id",
        component=(ibis.row_number() + max_existing_label + 1).cast("uint64"),
    )
    return additional_labels
