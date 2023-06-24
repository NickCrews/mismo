from __future__ import annotations

import altair as alt
import ibis
from ibis import _
from ibis.expr.types import Table
import numpy as np
import pandas as pd
from sklearn.manifold import MDS


def plot_cluster(nodes: Table, edges: Table) -> alt.Chart:
    """Plot a cluster of records and the links between them.

    Args:
        nodes: A table of records with at least columns (record_id, label_true)
            and optionally other columns.
        edges: A table of edges with at least columns
            (record_id_l, record_id_r, dissimilarity) and optionally other columns.
            Dissimilarity is like distance. See
            https://scikit-learn.org/stable/modules/generated/sklearn.manifold.MDS.html
    """
    nodes, edges = drop_orphaned(nodes, edges)
    nodes_with_coords = _layout_nodes(nodes, edges)
    return _plot_cluster(nodes_with_coords, edges)


def drop_orphaned(nodes: Table, edges: Table):
    nodes = nodes[
        _.record_id.isin(edges.record_id_l) | _.record_id.isin(edges.record_id_r)
    ]
    # edges = edges[
    #     _.record_id_l.isin(nodes.record_id) & _.record_id_r.isin(nodes.record_id)
    # ]
    return nodes, edges


def _layout_nodes(nodes: Table, edges: Table) -> Table:
    """Add columns x and y to nodes table that are the coordinates of the nodes"""
    n, e = _reindex_from_0(nodes, edges)
    dis_matrix = _edges_to_dissimilarity_matrix(n, e)
    mds = MDS(
        n_components=2,
        metric=False,
        random_state=42,
        dissimilarity="precomputed",
        normalized_stress="auto",
    )
    coords_np = mds.fit_transform(dis_matrix)
    coords_df = pd.DataFrame(coords_np, columns=["x", "y"])
    coords_df["id_in_cluster"] = range(len(coords_df))
    coords_table = ibis.memtable(coords_df)
    augmented = n.left_join(coords_table, "id_in_cluster").drop(
        "id_in_cluster_x", "id_in_cluster_y"
    )
    return augmented


def _edges_to_dissimilarity_matrix(nodes, edges):
    """Take an Ibis Table edge list with columns (id_l, id_r, dissimilarity) and return
    a square pandas DF of dissimilarities between nodes.

    This dissimilarity matrix is suitable for input to
    https://scikit-learn.org/stable/modules/generated/sklearn.manifold.MDS.html#sklearn.manifold.MDS.fit
    """
    edges_df: pd.DataFrame = edges[
        "id_in_cluster_l", "id_in_cluster_r", "dissimilarity"
    ].execute()

    # start with square matrix
    default_value = 1.0
    n = nodes.count().execute()
    matrix = np.full((n, n), default_value, dtype=np.float32)
    np.fill_diagonal(matrix, 0.0)

    # fill in the matrix with dissimilarities
    x = edges_df.id_in_cluster_l.values
    y = edges_df.id_in_cluster_r.values
    vals = edges_df.dissimilarity.values
    matrix[x, y] = vals
    matrix[y, x] = vals
    return matrix


def _reindex_from_0(nodes, edges):
    nodes = nodes.mutate(id_in_cluster=ibis.row_number())
    m = nodes["record_id", "id_in_cluster"]
    edges = edges.left_join(m, m.record_id == edges.record_id_l).drop("record_id")
    edges = edges.left_join(
        m, m.record_id == edges.record_id_r, suffixes=("_l", "_r")
    ).drop("record_id")
    return nodes, edges


def _plot_cluster(nodes_chart: Table, edges_chart: Table) -> alt.Chart:
    alt.data_transformers.disable_max_rows()

    nodes_df: pd.DataFrame = nodes_chart.execute()
    edges_df: pd.DataFrame = edges_chart.execute()

    nodes_df["record_id_a"] = nodes_df["record_id"]
    nodes_df["record_id_b"] = nodes_df["record_id"]

    edge_hovered = alt.selection_point(
        name="edge_hovered",
        on="mouseover",
        nearest=False,
        fields=["record_id_l", "record_id_r"],
        empty=True,
    )

    node_clicked = alt.selection_point(
        name="node_clicked", on="click", nearest=False, fields=["record_id"], empty=True
    )
    node_hovered = alt.selection_point(
        name="node_hovered",
        on="mouseover",
        nearest=False,
        fields=["record_id"],
        empty=False,
    )
    true_label_selected = alt.selection_point(
        name="true_label_selector", fields=["label_true"], bind="legend", empty=True
    )

    nodes_lookup = alt.LookupData(
        nodes_df, key="record_id", fields=["x", "y", "label_true"]
    )

    either_endpoint_label_selected = f"(datum.label_true_l == {true_label_selected.name}.label_true) || (datum.label_true_r == {true_label_selected.name}.label_true)"  # noqa: E501
    edge_hovered_expr = f"datum.record_id_l == {edge_hovered.name}.record_id_l && datum.record_id_r == {edge_hovered.name}.record_id_r"  # noqa: E501
    node_hovered_expr = f"datum.record_id_l == {node_hovered.name}.record_id || datum.record_id_r == {node_hovered.name}.record_id"  # noqa: E501
    node_clicked_expr = f"datum.record_id_l == {node_clicked.name}.record_id || datum.record_id_r == {node_clicked.name}.record_id"  # noqa: E501
    no_edge_hovered_expr = f"!isValid({edge_hovered.name}.record_id_l)"
    no_node_hovered_expr = f"!isValid({node_hovered.name}.record_id)"
    no_node_clicked_expr = f"!isValid({node_clicked.name}.record_id)"
    no_label_selected_expr = f"!isValid({true_label_selected.name}.label_true)"
    nothing_selected = f"({no_node_hovered_expr}) && ({no_node_clicked_expr}) && ({no_edge_hovered_expr}) && ({no_label_selected_expr})"  # noqa: E501
    is_edge_highlighted = alt.param(
        name="is_edge_highlighted",
        expr=f"({edge_hovered_expr}) || ({node_hovered_expr}) || ({node_clicked_expr}) || ({either_endpoint_label_selected}) ||({nothing_selected})",  # noqa: E501
    )

    edges_chart = (
        alt.Chart(edges_df)
        .mark_rule(opacity=1, strokeWidth=3)
        .encode(
            x="x_l:Q",
            y="y_l:Q",
            x2="x_r:Q",
            y2="y_r:Q",
            color=alt.condition(
                is_edge_highlighted,
                alt.Color("dissimilarity:Q", scale=alt.Scale(scheme="viridis")),
                alt.value("lightgray"),
            ),
            opacity=alt.condition(is_edge_highlighted, alt.value(1), alt.value(0.2)),
            tooltip=edges_df.columns.tolist(),
        )
        .transform_lookup(
            lookup="record_id_l",
            from_=nodes_lookup,
            as_=["x_l", "y_l", "label_true_l"],
        )
        .transform_lookup(
            lookup="record_id_r",
            from_=nodes_lookup,
            as_=["x_r", "y_r", "label_true_r"],
        )
        .add_params(edge_hovered)
    )

    node_tooltip_columns = set(nodes_df.columns)
    node_tooltip_columns -= {"x", "y", "label"}
    node_tooltip_columns = list(node_tooltip_columns)

    node_highlighted = node_hovered | node_clicked

    nodes_chart = (
        alt.Chart(nodes_df)
        .mark_circle()
        .encode(
            x="x:Q",
            y="y:Q",
            color=alt.condition(
                node_highlighted,
                alt.Color("label_true:N"),
                alt.value("lightgray"),
            ),
            tooltip=node_tooltip_columns,
        )
        .add_params(node_hovered)
        .add_params(node_clicked)
        .add_params(true_label_selected)
    )

    chart = edges_chart + nodes_chart
    return chart.interactive()
