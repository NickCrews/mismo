from __future__ import annotations

from typing import TYPE_CHECKING

import ibis
from ibis import _
from ibis.expr import types as ir

if TYPE_CHECKING:
    import altair as alt
    import ipywidgets


def distribution_chart(vals: ir.Column, *, limit: int | None = None) -> alt.Chart:
    """Make a Altair histogram of values.

    Useful as an exploratory tool to look at what values are present in a column.

    Parameters
    ----------
    vals : ColumnExpr
        The values to plot.
    limit : int, optional
        The maximum number of bars to plot, by default 1000

    Returns
    -------
    alt.Chart
        The histogram.
    """
    import altair as alt

    if limit is None:
        limit = 100
    counts = _make_counts(vals)
    counts = counts.mutate(label=_.value.cast("string"))
    df = counts.to_pandas()
    n_values = df.n.sum()
    n_unique = len(df)
    n_nulls = df[df.value.isnull()].n.sum()
    subset = df.iloc[:limit]

    col = vals.get_name()
    scrubber = alt.selection_interval(encodings=["x"], empty=True)
    x_sort = alt.EncodingSortField("rank", order="ascending")
    overview = (
        alt.Chart(
            subset,
            title=alt.Title(
                text="<Drag to select>",
                dy=30,
                anchor="middle",
                fontSize=12,
                color="gray",
            ),
            width=800,
            height=50,
        )
        .mark_area(interpolate="step-after")
        .encode(
            x=alt.X("label:N", sort=x_sort, axis=None),
            y=alt.Y("n:Q", title=None, axis=None),
        )
        .add_params(scrubber)
    )
    zoomin = (
        alt.Chart(subset, width=800, height=100)
        .mark_bar()
        .encode(
            x=alt.X(
                "label:N",
                sort=x_sort,
                axis=alt.Axis(title=col),
            ),
            y=alt.Y(
                "n:Q",
                title="Count",
                axis=alt.Axis(format="~s"),
            ),
            tooltip=[
                alt.Tooltip("label", title=col),
                alt.Tooltip("n", format="~s", title="Count"),
                alt.Tooltip("pct", format="%", title="Percent"),
            ],
            color=alt.Color("value:N", legend=None),
        )
        .transform_filter(scrubber)
    )
    together = overview & zoomin
    together = together.resolve_scale(color="independent")
    together = together.properties(
        title=alt.Title(
            f"Counts of column '{col}'",
            subtitle=f"showing top {limit:,} values out of {n_values:,} total, {n_unique:,} unique, {n_nulls:,} nulls",  # noqa: E501
            anchor="middle",
            fontSize=14,
        )
    )
    return together


def distribution_dashboard(
    records: ir.Table,
    *,
    column: str | None = None,
    limit: int | None = None,
) -> ipywidgets.VBox:
    """Make an ipywidget dashboard for exploring the distribution of values in a table.

    Parameters
    ----------
    records : Table
        The table to plot.
    column : str, optional
        The initial column to plot. If None, the first column is used.
        You can change this interactively in the returned dashboard.
    limit : int, optional
        The initial maximum number of values to plot, by default 100.
        You can change this interactively in the returned dashboard.

    Returns
    -------
    ipywidgets.VBox
        The dashboard.
    """
    import altair as alt
    import ipywidgets

    if column is None:
        column = records.columns[0]
    limit_max = int(records.count().execute())
    if limit is None:
        limit = min(limit_max, 100)
    column_selector = ipywidgets.Dropdown(
        options=records.columns,
        value=column,
        description="Column:",
        disabled=False,
    )
    limit_selector = ipywidgets.IntSlider(
        min=1, max=limit_max, value=limit, step=1, description="Limit:"
    )

    def get_chart():
        return distribution_chart(
            records[column_selector.value], limit=limit_selector.value
        )

    jupyter_chart = alt.JupyterChart(get_chart())

    def on_change(change):
        jupyter_chart.chart = get_chart()

    column_selector.observe(on_change, names="value")
    limit_selector.observe(on_change, names="value")

    layout = ipywidgets.VBox([column_selector, limit_selector, jupyter_chart])
    return layout


def _make_counts(vals: ir.Column) -> ir.Table:
    vals = vals.name("value")
    counts = vals.value_counts().rename(n="value_count")
    sort_keys = [_.n.desc(), _.value.asc()]
    counts = counts.order_by(sort_keys)
    counts = counts.mutate(rank=ibis.row_number(), pct=_.n / _.n.sum())
    return counts
