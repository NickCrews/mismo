"""Vega-Lite chart specifications for linkage analysis.

All functions return plain Python dicts that are valid Vega-Lite specs.
They can be used in two ways:

**Python / Altair:**

.. code-block:: python

    import altair as alt
    from mismo import analysis

    spec = analysis.charts.link_count(linkage)
    chart = alt.Chart.from_dict(spec)
    chart.show()

**JavaScript / Vega-Embed:**

.. code-block:: javascript

    const spec = await fetch("/api/charts/link_count").then(r => r.json());
    vegaEmbed("#view", spec);

All data is embedded inline in the spec so charts are self-contained.
"""

from __future__ import annotations

from typing import Any

from mismo.linkage._linkage import Linkage

_VEGA_LITE_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"


def _to_records(table: Any) -> list[dict]:
    """Execute an ibis table and return as a list of dicts."""
    import pandas as pd

    df = table.execute() if hasattr(table, "execute") else table
    if isinstance(df, pd.DataFrame):
        # Convert NA/NaN to None for JSON compatibility
        return df.where(df.notna(), other=None).to_dict(orient="records")
    return list(df)


def link_count(
    linkage: Linkage,
    side: str = "left",
    *,
    title: str | None = None,
    log_scale: bool = True,
) -> dict:
    """Bar chart showing how many records have 0, 1, 2, ... links.

    The X axis shows the number of links; the Y axis shows how many
    records have that count (log scale by default).

    Parameters
    ----------
    linkage
        The linkage to chart.
    side
        "left" or "right".
    title
        Override the chart title.
    log_scale
        Use a symlog Y axis (handles zeros).

    Returns
    -------
    A Vega-Lite spec dict.
    """
    table = linkage.left if side == "left" else linkage.right
    counts = table.link_counts().order_by("n_links")
    data = _to_records(counts)
    total = sum(r["n_records"] for r in data)
    for r in data:
        r["pct"] = round(r["n_records"] / total * 100, 1) if total > 0 else 0

    chart_title = title or f"Link Count Distribution ({side.title()} Table)"

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": chart_title,
            "subtitle": f"{total:,} total records",
            "anchor": "middle",
        },
        "width": 600,
        "height": 350,
        "data": {"values": data},
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "x": {
                "field": "n_links",
                "type": "ordinal",
                "title": "Number of Links",
                "sort": "ascending",
            },
            "y": {
                "field": "n_records",
                "type": "quantitative",
                "title": "Number of Records",
                "scale": {"type": "symlog"} if log_scale else {},
            },
            "color": {
                "condition": [
                    {"test": "datum.n_links === 0", "value": "#d62728"},
                    {"test": "datum.n_links === 1", "value": "#2ca02c"},
                ],
                "value": "#ff7f0e",
            },
            "tooltip": [
                {"field": "n_links", "title": "Links", "type": "ordinal"},
                {
                    "field": "n_records",
                    "title": "Records",
                    "type": "quantitative",
                    "format": ",",
                },
                {"field": "pct", "title": "% of Total", "format": ".1f"},
            ],
        },
    }


def coverage_donut(linkage: Linkage, *, title: str | None = None) -> dict:
    """Side-by-side donut charts showing linked vs unlinked coverage.

    Each donut shows the split between unlinked, singly-linked, and
    multiply-linked records for one side of the linkage.

    Parameters
    ----------
    linkage
        The linkage to chart.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict with two faceted donuts.
    """
    from mismo.analysis._records import with_link_category

    def _counts(side: str) -> list[dict]:
        cat_table = with_link_category(linkage, side)
        df = cat_table.group_by("link_category").aggregate(
            count=cat_table.link_category.count()
        ).execute()
        total = df["count"].sum()
        rows = []
        for _, row in df.iterrows():
            rows.append(
                {
                    "side": side,
                    "category": row["link_category"],
                    "count": int(row["count"]),
                    "pct": round(float(row["count"]) / total * 100, 1)
                    if total > 0
                    else 0,
                }
            )
        return rows

    data = _counts("left") + _counts("right")

    color_scale = {
        "domain": ["unlinked", "singly_linked", "multiply_linked"],
        "range": ["#d62728", "#2ca02c", "#ff7f0e"],
    }

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {"text": title or "Linkage Coverage", "anchor": "middle"},
        "data": {"values": data},
        "facet": {
            "field": "side",
            "type": "nominal",
            "title": "Side",
            "sort": ["left", "right"],
        },
        "spec": {
            "width": 240,
            "height": 240,
            "mark": {"type": "arc", "innerRadius": 60, "tooltip": True},
            "encoding": {
                "theta": {
                    "field": "count",
                    "type": "quantitative",
                    "stack": True,
                },
                "color": {
                    "field": "category",
                    "type": "nominal",
                    "title": "Category",
                    "scale": color_scale,
                    "sort": ["unlinked", "singly_linked", "multiply_linked"],
                },
                "tooltip": [
                    {"field": "side", "title": "Side"},
                    {"field": "category", "title": "Category"},
                    {
                        "field": "count",
                        "title": "Records",
                        "type": "quantitative",
                        "format": ",",
                    },
                    {"field": "pct", "title": "Pct %", "format": ".1f"},
                ],
            },
        },
    }


def score_histogram(
    linkage: Linkage,
    column: str,
    *,
    bins: int = 40,
    title: str | None = None,
) -> dict:
    """Histogram of a numeric attribute on the links table.

    Useful for visualizing match score distributions, similarity values,
    or other continuous link attributes.

    Parameters
    ----------
    linkage
        The linkage to chart.
    column
        Name of a numeric column on the links table.
    bins
        Number of histogram bins.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    if column not in linkage.links.columns:
        raise ValueError(
            f"Column {column!r} not in links. Available: {linkage.links.columns}"
        )
    data = _to_records(linkage.links.select(column))

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"Distribution of '{column}'",
            "anchor": "middle",
        },
        "width": 600,
        "height": 300,
        "data": {"values": data},
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "x": {
                "field": column,
                "type": "quantitative",
                "bin": {"maxbins": bins},
                "title": column,
            },
            "y": {
                "aggregate": "count",
                "type": "quantitative",
                "title": "Count",
            },
            "tooltip": [
                {
                    "field": column,
                    "bin": {"maxbins": bins},
                    "title": column,
                    "format": ".3f",
                },
                {"aggregate": "count", "title": "Links", "format": ","},
            ],
        },
    }


def link_attribute_bar(
    linkage: Linkage,
    column: str,
    *,
    top_n: int = 20,
    title: str | None = None,
) -> dict:
    """Bar chart showing counts of each value in a categorical link column.

    Useful for analyzing which blocking keys, match levels, or linker
    labels are most common.

    Parameters
    ----------
    linkage
        The linkage to chart.
    column
        Name of a categorical column on the links table.
    top_n
        Only show the top N most common values.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    from mismo.analysis._summary import link_attribute_counts

    rows = link_attribute_counts(linkage, column)
    data = rows[:top_n]
    total = sum(r["count"] for r in rows)

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"Link Counts by '{column}'",
            "subtitle": f"{total:,} total links",
            "anchor": "middle",
        },
        "width": 600,
        "height": 350,
        "data": {"values": data},
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "x": {
                "field": "count",
                "type": "quantitative",
                "title": "Number of Links",
            },
            "y": {
                "field": column,
                "type": "nominal",
                "title": column,
                "sort": "-x",
            },
            "color": {
                "field": "fraction",
                "type": "quantitative",
                "title": "Fraction",
                "scale": {"scheme": "blues"},
            },
            "tooltip": [
                {"field": column, "title": column},
                {"field": "count", "title": "Links", "format": ","},
                {"field": "fraction", "title": "Fraction", "format": ".2%"},
            ],
        },
    }


def field_comparison(
    linkage: Linkage,
    column: str,
    *,
    bins: int = 30,
    title: str | None = None,
) -> dict:
    """Overlaid histograms comparing a field's values in linked pairs.

    Shows the distribution of a column from the left side vs the right
    side for all linked pairs. When the distributions overlap well, your
    linker is pairing similar records; divergence indicates issues.

    The column must exist on both left and right tables.

    Parameters
    ----------
    linkage
        The linkage to chart.
    column
        A column name that exists on both left and right tables.
    bins
        Number of histogram bins.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    import ibis
    from ibis import _

    left_t = linkage.left
    right_t = linkage.right
    if column not in left_t.columns:
        raise ValueError(f"Column {column!r} not in left table")
    if column not in right_t.columns:
        raise ValueError(f"Column {column!r} not in right table")

    links = linkage.links
    left_vals = (
        links.join(left_t, links.record_id_l == left_t.record_id)
        .select(value=left_t[column])
        .mutate(side=ibis.literal("left"))
    )
    right_vals = (
        links.join(right_t, links.record_id_r == right_t.record_id)
        .select(value=right_t[column])
        .mutate(side=ibis.literal("right"))
    )
    data = _to_records(ibis.union(left_vals, right_vals))

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"'{column}' in Linked Pairs",
            "subtitle": "Left vs Right distribution",
            "anchor": "middle",
        },
        "width": 600,
        "height": 300,
        "data": {"values": data},
        "mark": {"type": "bar", "opacity": 0.6, "tooltip": True},
        "encoding": {
            "x": {
                "field": "value",
                "type": "quantitative",
                "bin": {"maxbins": bins},
                "title": column,
            },
            "y": {
                "aggregate": "count",
                "type": "quantitative",
                "title": "Count",
                "stack": None,
            },
            "color": {
                "field": "side",
                "type": "nominal",
                "title": "Side",
                "scale": {
                    "domain": ["left", "right"],
                    "range": ["#4c78a8", "#f58518"],
                },
            },
            "tooltip": [
                {"field": "side", "title": "Side"},
                {
                    "field": "value",
                    "bin": {"maxbins": bins},
                    "title": column,
                },
                {"aggregate": "count", "title": "Count", "format": ","},
            ],
        },
    }


def link_count_ecdf(
    linkage: Linkage,
    side: str = "left",
    *,
    title: str | None = None,
) -> dict:
    """ECDF: what fraction of records have at most K links.

    The curve shows cumulative coverage as a function of link count.
    A steep jump at n_links=1 means most records have exactly one link.
    A flat tail at high n_links means few records are highly linked.

    Parameters
    ----------
    linkage
        The linkage to chart.
    side
        "left" or "right".
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    table = linkage.left if side == "left" else linkage.right
    with_n = table.with_n_links()
    data = _to_records(with_n.select("n_links").order_by("n_links"))
    total = len(data)
    for r in data:
        r["pct"] = round(r["n_links"] / total * 100, 2) if total > 0 else 0

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"Cumulative Link Coverage ({side.title()})",
            "subtitle": "What fraction of records have ≤ K links",
            "anchor": "middle",
        },
        "width": 600,
        "height": 300,
        "data": {"values": data},
        "mark": {"type": "line", "interpolate": "step-after"},
        "transform": [
            {
                "sort": [{"field": "n_links", "order": "ascending"}],
                "window": [
                    {
                        "op": "cume_dist",
                        "as": "cumulative_fraction",
                    }
                ],
            }
        ],
        "encoding": {
            "x": {
                "field": "n_links",
                "type": "quantitative",
                "title": "Number of Links (K)",
            },
            "y": {
                "field": "cumulative_fraction",
                "type": "quantitative",
                "title": "Fraction of Records with ≤ K links",
                "axis": {"format": ".0%"},
            },
        },
    }


def pair_scatter(
    linkage: Linkage,
    x_column: str,
    y_column: str,
    *,
    side: str = "left",
    max_points: int = 2000,
    title: str | None = None,
) -> dict:
    """Scatter plot of two numeric columns for records in the linkage.

    Each point is a record. Color shows link category (unlinked,
    singly linked, multiply linked). Useful for seeing if unlinked
    records cluster in a specific region of feature space.

    Parameters
    ----------
    linkage
        The linkage to chart.
    x_column
        Column name for the X axis.
    y_column
        Column name for the Y axis.
    side
        "left" or "right".
    max_points
        Max records to plot (sampled for large tables).
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    from mismo.analysis._records import with_link_category

    cat_table = with_link_category(linkage, side)
    available = cat_table.columns
    for col in (x_column, y_column):
        if col not in available:
            raise ValueError(
                f"Column {col!r} not found. Available: {available}"
            )

    sample = cat_table.select(x_column, y_column, "link_category").limit(
        max_points
    )
    data = _to_records(sample)

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"{x_column} vs {y_column} ({side.title()})",
            "subtitle": "Colored by link category",
            "anchor": "middle",
        },
        "width": 500,
        "height": 400,
        "data": {"values": data},
        "mark": {"type": "point", "opacity": 0.6, "tooltip": True},
        "encoding": {
            "x": {
                "field": x_column,
                "type": "quantitative",
                "title": x_column,
            },
            "y": {
                "field": y_column,
                "type": "quantitative",
                "title": y_column,
            },
            "color": {
                "field": "link_category",
                "type": "nominal",
                "title": "Category",
                "scale": {
                    "domain": [
                        "unlinked",
                        "singly_linked",
                        "multiply_linked",
                    ],
                    "range": ["#d62728", "#2ca02c", "#ff7f0e"],
                },
            },
            "tooltip": [
                {"field": x_column, "type": "quantitative"},
                {"field": y_column, "type": "quantitative"},
                {"field": "link_category", "title": "Category"},
            ],
        },
        "selection": {
            "grid": {
                "type": "interval",
                "bind": "scales",
            }
        },
    }


def n_links_heatmap(
    linkage: Linkage,
    *,
    max_links_shown: int = 10,
    title: str | None = None,
) -> dict:
    """Heatmap of left link count vs right link count.

    Each cell (i, j) shows how many *link pairs* exist where the left
    record has i links and the right record has j links. This reveals
    structural patterns, e.g. how often a many-to-many situation occurs.

    Parameters
    ----------
    linkage
        The linkage to chart.
    max_links_shown
        Clip link counts at this value (merges rare high values).
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    import ibis
    from ibis import _

    left_n = linkage.left.with_n_links().select(
        record_id_l="record_id", n_links_l="n_links"
    )
    right_n = linkage.right.with_n_links().select(
        record_id_r="record_id", n_links_r="n_links"
    )
    pairs = (
        linkage.links.select("record_id_l", "record_id_r")
        .join(left_n, "record_id_l")
        .join(right_n, "record_id_r")
        .mutate(
            n_links_l=ibis.least(_.n_links_l, max_links_shown),
            n_links_r=ibis.least(_.n_links_r, max_links_shown),
        )
        .group_by("n_links_l", "n_links_r")
        .aggregate(count=_.count())
    )
    data = _to_records(pairs)
    max_count = max((r["count"] for r in data), default=1)

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or "Link Pairs: Left vs Right Link Count",
            "subtitle": f"Counts clipped at {max_links_shown}",
            "anchor": "middle",
        },
        "width": 400,
        "height": 400,
        "data": {"values": data},
        "mark": {"type": "rect", "tooltip": True},
        "encoding": {
            "x": {
                "field": "n_links_l",
                "type": "ordinal",
                "title": "Left Record's Link Count",
                "sort": "ascending",
            },
            "y": {
                "field": "n_links_r",
                "type": "ordinal",
                "title": "Right Record's Link Count",
                "sort": "ascending",
            },
            "color": {
                "field": "count",
                "type": "quantitative",
                "title": "Pairs",
                "scale": {
                    "scheme": "blues",
                    "domain": [0, max_count],
                },
            },
            "tooltip": [
                {
                    "field": "n_links_l",
                    "title": "Left link count",
                    "type": "ordinal",
                },
                {
                    "field": "n_links_r",
                    "title": "Right link count",
                    "type": "ordinal",
                },
                {"field": "count", "title": "Pairs", "format": ","},
            ],
        },
    }


def summary_table(
    linkage: Linkage,
    *,
    title: str | None = None,
) -> dict:
    """A Vega-Lite table visualization of summary statistics.

    Renders key scalars (n_left, n_right, n_links, coverage, etc.)
    as a formatted two-column table: metric name and value.

    Parameters
    ----------
    linkage
        The linkage to summarize.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    from mismo.analysis._summary import summary as _summary

    stats = _summary(linkage)
    pct_fmt = lambda v: f"{v * 100:.1f}%"  # noqa: E731

    rows = [
        {"metric": "Left records", "value": f"{stats['n_left']:,}"},
        {"metric": "Right records", "value": f"{stats['n_right']:,}"},
        {"metric": "Total links", "value": f"{stats['n_links']:,}"},
        {"metric": "Left coverage", "value": pct_fmt(stats["left_coverage"])},
        {
            "metric": "Left unlinked",
            "value": f"{stats['left_unlinked']:,}",
        },
        {
            "metric": "Left multiply-linked",
            "value": f"{stats['left_multiply_linked']:,}",
        },
        {
            "metric": "Left avg links",
            "value": f"{stats['left_avg_links']:.2f}",
        },
        {
            "metric": "Left max links",
            "value": f"{stats['left_max_links']:,}",
        },
        {
            "metric": "Right coverage",
            "value": pct_fmt(stats["right_coverage"]),
        },
        {
            "metric": "Right unlinked",
            "value": f"{stats['right_unlinked']:,}",
        },
        {
            "metric": "Right multiply-linked",
            "value": f"{stats['right_multiply_linked']:,}",
        },
        {
            "metric": "Right avg links",
            "value": f"{stats['right_avg_links']:.2f}",
        },
        {
            "metric": "Right max links",
            "value": f"{stats['right_max_links']:,}",
        },
    ]

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or "Linkage Summary",
            "anchor": "middle",
        },
        "width": 300,
        "height": 30 * len(rows),
        "data": {"values": rows},
        "layer": [
            {
                "mark": {
                    "type": "rect",
                    "color": {"expr": "indexof(data('source_0').map(d => d.metric), datum.metric) % 2 === 0 ? '#f5f5f5' : 'white'"},
                }
            },
            {
                "encoding": {
                    "y": {
                        "field": "metric",
                        "type": "ordinal",
                        "sort": None,
                        "axis": {"title": None, "labelLimit": 200},
                    }
                },
                "layer": [
                    {
                        "mark": {"type": "text", "align": "left", "dx": -120},
                        "encoding": {
                            "text": {"field": "metric"},
                            "color": {"value": "#333"},
                        },
                    },
                    {
                        "mark": {"type": "text", "align": "right", "dx": 120},
                        "encoding": {
                            "text": {"field": "value"},
                            "color": {"value": "#000"},
                            "fontWeight": {"value": "bold"},
                        },
                    },
                ],
            },
        ],
    }


def score_threshold_impact(
    linkage: Linkage,
    score_column: str,
    *,
    n_thresholds: int = 50,
    title: str | None = None,
) -> dict:
    """Line chart showing how link counts change with a score threshold.

    Sweeps threshold values from min to max, showing how many links
    remain above each threshold. Useful for choosing a cutoff.

    Parameters
    ----------
    linkage
        The linkage to chart.
    score_column
        A numeric column on the links table.
    n_thresholds
        How many threshold values to sample.
    title
        Override the chart title.

    Returns
    -------
    A Vega-Lite spec dict.
    """
    import numpy as np

    if score_column not in linkage.links.columns:
        raise ValueError(
            f"Column {score_column!r} not in links. "
            f"Available: {linkage.links.columns}"
        )

    stats = linkage.links.aggregate(
        min_val=linkage.links[score_column].min(),
        max_val=linkage.links[score_column].max(),
    ).execute().iloc[0]
    min_val = float(stats["min_val"])
    max_val = float(stats["max_val"])

    thresholds = np.linspace(min_val, max_val, n_thresholds).tolist()
    all_scores = (
        linkage.links.select(score_column).execute()[score_column].tolist()
    )
    total = len(all_scores)

    rows = []
    for t in thresholds:
        above = sum(1 for s in all_scores if s is not None and s >= t)
        rows.append(
            {
                "threshold": round(t, 4),
                "n_links": above,
                "fraction": round(above / total, 4) if total > 0 else 0,
            }
        )

    return {
        "$schema": _VEGA_LITE_SCHEMA,
        "title": {
            "text": title or f"Link Count vs '{score_column}' Threshold",
            "subtitle": "Drag threshold to see impact",
            "anchor": "middle",
        },
        "width": 600,
        "height": 300,
        "data": {"values": rows},
        "layer": [
            {
                "mark": {"type": "line"},
                "encoding": {
                    "x": {
                        "field": "threshold",
                        "type": "quantitative",
                        "title": f"Minimum {score_column}",
                    },
                    "y": {
                        "field": "n_links",
                        "type": "quantitative",
                        "title": "Links Remaining",
                    },
                },
            },
            {
                "mark": {"type": "line", "strokeDash": [5, 5], "color": "gray"},
                "encoding": {
                    "x": {
                        "field": "threshold",
                        "type": "quantitative",
                    },
                    "y": {
                        "field": "fraction",
                        "type": "quantitative",
                        "title": "Fraction Remaining",
                        "axis": {"format": ".0%"},
                    },
                },
            },
        ],
        "resolve": {"scale": {"y": "independent"}},
    }


def available_charts() -> list[dict[str, str]]:
    """List all available chart functions with their descriptions.

    Returns
    -------
    List of dicts with 'name', 'description', and 'required_args'.
    """
    return [
        {
            "name": "link_count",
            "description": "Bar chart: how many records have 0, 1, 2, ... links",
            "required_args": [],
            "optional_args": ["side", "title", "log_scale"],
        },
        {
            "name": "coverage_donut",
            "description": "Donut charts: unlinked vs singly- vs multiply-linked for each side",
            "required_args": [],
            "optional_args": ["title"],
        },
        {
            "name": "score_histogram",
            "description": "Histogram of a numeric link attribute (e.g. match score)",
            "required_args": ["column"],
            "optional_args": ["bins", "title"],
        },
        {
            "name": "link_attribute_bar",
            "description": "Bar chart: counts of each value in a categorical link column",
            "required_args": ["column"],
            "optional_args": ["top_n", "title"],
        },
        {
            "name": "field_comparison",
            "description": "Overlaid histograms of a field in left vs right of linked pairs",
            "required_args": ["column"],
            "optional_args": ["bins", "title"],
        },
        {
            "name": "link_count_ecdf",
            "description": "ECDF: cumulative fraction of records with at most K links",
            "required_args": [],
            "optional_args": ["side", "title"],
        },
        {
            "name": "pair_scatter",
            "description": "Scatter plot of two numeric columns, colored by link category",
            "required_args": ["x_column", "y_column"],
            "optional_args": ["side", "max_points", "title"],
        },
        {
            "name": "n_links_heatmap",
            "description": "Heatmap: left link count vs right link count for all pairs",
            "required_args": [],
            "optional_args": ["max_links_shown", "title"],
        },
        {
            "name": "summary_table",
            "description": "Vega-Lite table of key summary statistics",
            "required_args": [],
            "optional_args": ["title"],
        },
        {
            "name": "score_threshold_impact",
            "description": "Line chart: how link count changes with a numeric score threshold",
            "required_args": ["score_column"],
            "optional_args": ["n_thresholds", "title"],
        },
    ]
