from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Iterable, Mapping

import ibis
from ibis import _
from ibis.expr import types as ir
from IPython.display import display
import ipywidgets
import rich
import solara

from mismo import _util
from mismo._datasets import Datasets
from mismo.cluster._connected_components import connected_components

if TYPE_CHECKING:
    import ipycytoscape  # type: ignore


def cytoscape_widget(
    tables: Datasets | ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> ipycytoscape.CytoscapeWidget:
    """Make a ipycytoscape.CytoscapeWidget that shows records and links.

    This shows ALL the supplied records and links, so be careful,
    you probably want to filter them down first.

    Parameters
    ----------
    tables :
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
    with _util.optional_import("ipycytoscape"):
        import ipycytoscape  # type: ignore

    ds = Datasets(tables)
    links = _filter_links(links, ds)
    graph = {"nodes": _nodes_to_json(ds), "edges": _edges_to_json(links)}
    cyto = ipycytoscape.CytoscapeWidget(graph, layout={"name": "fcose"})
    style = [
        *cyto.get_style(),
        {
            "selector": "node",
            "css": {
                "label": "data(label)",
                "font-size": 8,
                "color": "data(color)",
                "width": 15,
                "height": 15,
            },
        },
        {
            "selector": "edge",
            "css": {
                "curve-style": "straight",
                "width": "data(width)",
                "opacity": "data(opacity)",
            },
        },
    ]
    cyto.set_style(style)
    return cyto


def _filter_links(links: ir.Table, ds: Datasets) -> ir.Table:
    if "record_id_l" not in links.columns:
        raise ValueError("links must have a record_id_l column")
    if "record_id_r" not in links.columns:
        raise ValueError("links must have a record_id_r column")
    links = links.filter(
        _.record_id_l.isin(ds.all_record_ids()),
        _.record_id_r.isin(ds.all_record_ids()),
    )
    return links


def _nodes_to_json(ds: Datasets) -> list[dict[str, Any]]:
    colors = ["blue", "red", "green"]
    cmap = dict(zip(ds.names, colors[: len(ds)]))

    def f(name: str, t: ir.Table) -> ir.Table:
        m = {
            "dataset": ibis.literal(name),
            "id": _.record_id.cast(str),
        }
        if "label" not in t.columns:
            m["label"] = name + ":" + _.record_id.cast(str)
        if "color" not in t.columns:
            m["color"] = ibis.literal(cmap[name])
        return t.mutate(m)

    return _to_json(*ds.map(f))


def _edges_to_json(links: ir.Table) -> list[dict[str, Any]]:
    def _ensure_has_width(links: ir.Table) -> ir.Table:
        if "width" in links.columns:
            return links
        if "odds" not in links.columns:
            return links.mutate(width=5)
        log_odds = _.odds.log10()
        log_odds_fraction = log_odds / log_odds.max()
        width = 10 * log_odds_fraction
        return links.mutate(width=width)

    def _ensure_has_opacity(links: ir.Table) -> ir.Table:
        if "opacity" in links.columns:
            return links
        return links.mutate(opacity=0.5)

    if "source" in links.columns:
        raise ValueError("links must not have a source column")
    if "target" in links.columns:
        raise ValueError("links must not have a target column")
    links = links.mutate(source="record_id_l", target="record_id_r")
    links = _ensure_has_width(links)
    links = _ensure_has_opacity(links)
    return _to_json(links)


def _to_json(*tables: ir.Table) -> list[dict[str, Any]]:
    records = []
    for t in tables:
        # include default_handler to avoid https://stackoverflow.com/a/60492211/5156887
        json_str = t.to_pandas().to_json(
            orient="records", default_handler=str, date_format="iso"
        )
        records.extend(json.loads(json_str))
    return records


def _render_to_html(obj) -> str:
    console = rich.console.Console(record=True)
    with console.capture():
        console.print(obj)
    template = """
    <pre style="font-family:Menlo,'DejaVu Sans Mono',consolas,'Courier New',monospace; line-height: 1.2em">
        <code style="font-family:inherit">{code}</code>
    </pre>
    """  # noqa: E501
    return console.export_html(code_format=template, inline_styles=True)


@solara.component
def cluster_dashboard(
    ds: Datasets | ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> solara.Column:
    ds = Datasets(ds)

    def get_output():
        out = ipywidgets.Output()
        out.append_display_data(ipywidgets.HTML("Select a node or edge..."))
        return out

    info = solara.use_memo(get_output)

    def make_cyto() -> tuple[Any, dict[Any, dict]]:
        cyto = cytoscape_widget(ds, links)
        lookup = {r["record_id"]: r for r in _nodes_to_json(ds)}

        def on_record(node: dict[str, Any]):
            info.clear_output()
            html_widget = ipywidgets.HTML(_render_to_html(node["data"]))
            with info:
                display(html_widget)

        def on_edge(edge: dict[str, Any]):
            s = edge["data"]["record_id_l"]
            t = edge["data"]["record_id_r"]
            record_l = lookup[s]
            record_r = lookup[t]
            box = ipywidgets.HBox(
                [
                    ipywidgets.HTML(_render_to_html(record_l)),
                    ipywidgets.HTML(_render_to_html(record_r)),
                ]
            )
            info.clear_output()
            with info:
                display(box)

        cyto.on("node", "click", on_record)
        cyto.on("edge", "click", on_edge)
        return cyto

    cyto = solara.use_memo(make_cyto, [ds, links])

    return solara.Column([cyto, info])


@solara.component
def clusters_dashboard(
    tables: Datasets | ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> solara.Column:
    def get_ds():
        ds = Datasets(tables)
        li = _filter_links(links, ds)
        ds = connected_components(records=ds, links=li)
        ds = ds.cache()
        return ds

    ds = solara.use_memo(get_ds, [tables, links])

    def get_components():
        return (
            ds.unioned()
            .select("component")
            .distinct()
            .order_by("component")
            .component.execute()
            .to_list()
        )

    all_components = solara.use_memo(get_components, [ds])

    component = solara.use_reactive(all_components[0] if len(all_components) else None)
    component_selector = solara.Select(
        "Component", values=all_components, value=component
    )

    def get_subgraph():
        return ds.map(lambda name, t: t.filter(_.component == component.value))

    subgraph = solara.use_memo(get_subgraph, [ds, component.value])
    return solara.Column([component_selector, cluster_dashboard(subgraph, links)])
