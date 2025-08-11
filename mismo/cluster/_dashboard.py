from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Mapping

from ibis.expr import types as ir

from mismo._datasets import Datasets

if TYPE_CHECKING:
    import solara


def cluster_dashboard(
    ds: Datasets | ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> solara.Column:
    """A Solara component for that shows a cluster of records and links.

    This shows *ALL* the supplied records and links, so be careful,
    you probably want to filter them down first.
    You can use the `clusters_dashboard` component for that.

    This is like `cytoscape_widget`, but with a status bar that shows
    information about the selected node or edge.

    Parameters
    ----------
    ds :
        Table(s) of records with at least the column `record_id`.
    links :
        A table of edges with at least columns
        (record_id_l, record_id_r) and optionally other columns.
        The column `width` is used to set the width of the edges.
        If not given, it is determined from the column `odds`, if
        present, or set to 5 otherwise.
        The column `opacity` is used to set the opacity of the edges.
        If not given, it is set to 0.5.
    """
    from mismo.cluster._dashboard_internal import cluster_dashboard

    return cluster_dashboard(ds, links)


def clusters_dashboard(
    tables: Datasets | ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> solara.Column:
    """Make a dashboard for exploring different clusters of records.

    Pass the entire dataset and the links between records,
    and use this to filter down to a particular cluster.
    """
    from mismo.cluster._dashboard_internal import clusters_dashboard

    return clusters_dashboard(tables, links)
