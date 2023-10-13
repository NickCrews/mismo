from __future__ import annotations

import altair as alt
import ibis
from ibis import _
from ibis.expr.types import ColumnExpr, Table


def _make_counts(vals: ColumnExpr) -> Table:
    vals = vals.name("value")
    counts = vals.value_counts().rename(n="value_count")
    sort_keys = [_.n.desc(), _.value.asc()]
    counts = counts.order_by(sort_keys)
    counts = counts.mutate(rank=ibis.row_number())
    return counts


def histograms(vals: ColumnExpr, limit: int | None = None) -> alt.Chart:
    """Make a Altair histogram of the values in vals.

    Useful as an exploratory tool to look at what values are present in a column.

    Parameters
    ----------
    vals : ColumnExpr
        The values to plot.
    limit : int, optional
        The maximum number of bars to plot, by default 1000
    """
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
                title="Number of Appearances",
                axis=alt.Axis(format="~s"),
            ),
            tooltip=[
                alt.Tooltip("label", title=col),
                alt.Tooltip("n", format="~s", title="Number of Records"),
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


def distributions(records: Table, limit: int | None = None) -> alt.Chart:
    """For each column in records, make an Altair histogram.

    The "record_id" column is ignored.

    Parameters
    ----------
    records : Table
        The table to plot.

    Returns
    -------
    alt.Chart
        A chart with one histogram for each column in records.
    """
    sub_charts = []
    for col in records.columns:
        if col == "record_id":
            continue
        sub_charts.append(histograms(records[col], limit=limit))
    result = alt.vconcat(*sub_charts)
    result = result.resolve_legend(color="independent")
    return result
