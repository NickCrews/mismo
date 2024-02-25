from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from ibis import _
from ibis.expr import types as it
import ipywidgets
import rich

from mismo import _util

if TYPE_CHECKING:
    import ipycytoscape  # type: ignore


def cluster_widget(records: it.Table, links: it.Table) -> ipycytoscape.CytoscapeWidget:
    """Make a Widget that shows a cluster of records and the links between them.

    Uses ipycytoscape to make an interactive widget.

    Parameters
    ----------
    records :
        A table of records with at least the column (record_id).
    links :
        A table of edges with at least columns
        (record_id_l, record_id_r) and optionally other columns.
        The column `width` is used to set the width of the edges.
        If not given, it is determined from the column `odds`, if
        present, or set to 5 otherwise.
        The column `opacity` is used to set the opacity of the edges.
        If not given, it is set to 0.5.
    """
    with _util.optional_import("ipycytoscape"):
        import ipycytoscape  # type: ignore

    links = links[_.record_id_l != _.record_id_r]
    links = links[
        (_.record_id_l.isin(records.record_id))
        & (_.record_id_r.isin(records.record_id))
    ]
    links = _ensure_has_width(links)
    links = _ensure_has_opacity(links)
    if "record_id" not in records.columns:
        raise ValueError("records must have a record_id column")
    if "id" in records.columns:
        raise ValueError("records must not have an id column")
    if "record_id_l" not in links.columns:
        raise ValueError("links must have a record_id_l column")
    if "record_id_r" not in links.columns:
        raise ValueError("links must have a record_id_r column")
    if "source" in links.columns:
        raise ValueError("links must not have a source column")
    if "target" in links.columns:
        raise ValueError("links must not have a target column")
    graph = {
        "nodes": records.rename(id="record_id").to_pandas().to_dict("records"),
        "edges": links.rename(source="record_id_l", target="record_id_r")
        .to_pandas()
        .to_dict("records"),
    }
    cyto = ipycytoscape.CytoscapeWidget(graph, layout={"name": "fcose"})
    style = [
        *cyto.get_style(),
        {
            "selector": "node",
            "css": {
                "color": "black",
                "width": 15,
                "height": 15,
            },
        },
        {
            "selector": "edge",
            "css": {
                "curve-style": "straight",
                # "line-color": "data(odds)",
                "width": "data(width)",
                "opacity": "data(opacity)",
            },
        },
        {
            "selector": ":active",
            # "selector": ":hover",
            # "selector": ":mouseover",
            "css": {
                "content": "data(name)",
                "text-valign": "center",
                "color": "black",
            },
        },
    ]
    cyto.set_style(style)
    return cyto


def display_record(record: dict[str, Any]):
    rich.print(record)


def display_edge(
    record_l: dict[str, Any], record_r: dict[str, Any], edge: dict[str, Any]
):
    rich.print("Left:")
    rich.print(record_l)
    rich.print("Right:")
    rich.print(record_r)


def cluster_dashboard(
    records: it.Table,
    links: it.Table,
    *,
    display_record: Callable | None = display_record,
    display_edge: Callable | None = display_edge,
) -> ipywidgets.VBox:
    """Make a dashboard for exploring different clusters of records."""
    cyto = cluster_widget(records, links)
    info = ipywidgets.Output(layout={"height": "500px"})

    def _to_dict(t: it.Table, filter):
        one_row = t[filter]
        return one_row.execute().to_dict("records")[0]

    def on_record(node):
        node_dict = _to_dict(records, _.record_id == int(node["data"]["id"]))
        info.clear_output()
        with info:
            display_record(node_dict)

    def on_edge(edge):
        s = int(edge["data"]["source"])
        t = int(edge["data"]["target"])
        record_l = _to_dict(records, _.record_id == s)
        record_r = _to_dict(records, _.record_id == t)
        edge = _to_dict(links, (_.record_id_l == s) & (_.record_id_r == t))
        info.clear_output()
        with info:
            display_edge(record_l, record_r, edge)

    if display_record is not None:
        cyto.on("node", "mouseover", on_record)
    if display_edge is not None:
        cyto.on("edge", "mouseover", on_edge)
    return ipywidgets.VBox([cyto, info])


def _ensure_has_width(links: it.Table) -> it.Table:
    if "width" in links.columns:
        return links
    if "odds" not in links.columns:
        return links.mutate(width=5)
    log_odds = _.odds.log10()
    log_odds_fraction = log_odds / log_odds.max()
    width = 10 * log_odds_fraction
    return links.mutate(width=width)


def _ensure_has_opacity(links: it.Table) -> it.Table:
    if "opacity" in links.columns:
        return links
    return links.mutate(opacity=0.5)
