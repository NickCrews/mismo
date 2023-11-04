from __future__ import annotations

import colorsys
from typing import Iterable

import altair as alt
import ibis
from ibis import _
from ibis import selectors as s
from ibis.expr.types import IntegerValue, StringValue, Table

from mismo.compare import Comparison, Comparisons


def comparisons_histogram(compared: Table, comparisons: Comparisons) -> alt.Chart:
    """Plot a histogram of comparison vectors.

    Used to see which comparison levels are common, which are rare, and which
    Comparisons are related to each other. For example, exact matches should
    appear together across all Comparisons, this probably represents true
    matches.

    Parameters
    ----------
    compared : Table
        The result of a comparison.
    """
    cols = [comp.name for comp in comparisons]

    vector_counts = compared.group_by(cols).agg(n_pairs=_.count())
    vector_counts = vector_counts.mutate(pct_pairs=_.n_pairs / _.n_pairs.sum())
    vector_counts = vector_counts.order_by(_.n_pairs.desc())
    vector_counts = vector_counts.mutate(vector_id=ibis.row_number())
    vector_counts = vector_counts.cache()

    longer = vector_counts.pivot_longer(
        s.any_of(cols), names_to="comparison", values_to="level"
    )
    longer = longer.mutate(id=_.comparison + ":" + _.level)
    longer = longer.mutate(level_idx=_id_to_level_index(longer.id, comparisons))
    longer = longer.cache()

    width = 500
    scrubber_filter = alt.selection_interval(encodings=["x"])
    vector_fader = alt.selection_point(fields=["vector_id"], on="mouseover")

    opacity_vector = alt.condition(vector_fader, alt.value(1), alt.value(0.9))
    color_domain, color_range = _make_color_map(comparisons)

    hist = (
        alt.Chart(vector_counts, width=width)
        .mark_rect(
            stroke=None,
        )
        .encode(
            x=alt.X("vector_id:N", axis=None),
            y=alt.Y(
                "n_pairs:Q",
                title="Number of Pairs",
                scale=alt.Scale(type="log"),
            ),
            opacity=opacity_vector,
            tooltip=[
                alt.Tooltip("n_pairs", title="Number of Pairs", format=","),
                alt.Tooltip("pct_pairs", title="Percent of Pairs", format="%"),
                *cols,
            ],
        )
        .transform_filter(scrubber_filter)
        .add_params(vector_fader)
    )
    vector_chart = (
        alt.Chart(longer, height=80, width=width)
        .mark_rect()
        .encode(
            x=alt.X("vector_id:N", axis=None),
            y=alt.Y(
                "comparison",
                title="Comparison",
            ),
            color=alt.Color(
                "id",
                title="Comparison:Level",
                scale=alt.Scale(domain=color_domain, range=color_range),
            ),
            opacity=opacity_vector,
            tooltip=["level"],
        )
        .transform_filter(scrubber_filter)
        .add_params(vector_fader)
    )
    scrubber_chart = (
        alt.Chart(
            vector_counts,
            height=50,
            width=width,
            title=alt.Title(
                text="<Drag to select>",
                dy=20,
                anchor="middle",
                fontSize=12,
                color="gray",
            ),
        )
        .mark_rect()
        .encode(
            x=alt.X("vector_id:N", axis=None),
            y=alt.Y(
                "n_pairs:Q",
                title="Number of Pairs",
                scale=alt.Scale(type="log"),
                axis=None,
            ),
            opacity=opacity_vector,
        )
        .add_params(scrubber_filter)
        .add_params(vector_fader)
    )
    together = alt.vconcat(hist, vector_chart, scrubber_chart)
    together = together.properties(
        title=alt.Title(
            text="Distribution of Comparison Levels",
            subtitle=f"Total Pairs: {vector_counts.n_pairs.sum().execute():,}",
            anchor="middle",
            fontSize=14,
        )
    )
    return together


def _id_to_level_index(
    id: StringValue, comparisons: Iterable[Comparison]
) -> IntegerValue:
    cases = []
    for comparison in comparisons:
        for i, level in enumerate(comparison):
            cases.append((comparison.name + ":" + level.name, i))
    return id.cases(cases)


def _frange(start, stop, n):
    return [start + i * (stop - start) / n for i in range(n)]


def _make_color_map(comparisons: Comparisons) -> tuple[list, list]:
    domain = []
    range = []
    hues = _frange(0, 1, len(comparisons))
    for comp, hue in zip(comparisons, hues):
        level_names = [level.name for level in comp]
        shades = _frange(0.2, 0.9, len(level_names))
        for level, shade in zip(level_names, shades):
            r, g, b = colorsys.hsv_to_rgb(hue, 1, shade)
            r = int(r * 255)
            g = int(g * 255)
            b = int(b * 255)
            hex_color = f"#{r:02x}{g:02x}{b:02x}"
            domain.append(comp.name + ":" + level)
            range.append(hex_color)
    return domain, range


# TODO: make this work as a filter for the above histogram
def _make_legend_plot(longer: Table, color_map):
    levels = longer.group_by(["comparison", "level"]).agg(
        id=_.id.first(),
        level_idx=_.level_idx.first(),
        vector_ids=_.vector_id.collect(),
    )
    levels = levels.distinct()

    comparison_level_filter = alt.selection_point(fields=["comparison", "level"])
    # Use names based off of https://github.com/altair-viz/altair/issues/2366
    vector_ids_filter = alt.selection_point(fields=["vector_ids"], name="vidf")

    legend_base = (
        alt.Chart(levels.to_pandas(), height=100)
        .mark_rect()
        .encode(
            x=alt.X(
                "level_idx:N", axis=alt.Axis(title="Level", labels=False, ticks=False)
            ),
            y=alt.Y(
                "comparison",
                title="Comparison",
            ),
            opacity=alt.condition(
                comparison_level_filter, alt.value(1), alt.value(0.4)
            ),
            tooltip=["level"],
        )
    )
    legend_rects = legend_base.encode(
        color=alt.Color(
            "id",
            title="Comparison:Level",
            scale=alt.Scale(domain=color_map[0], range=color_map[1]),
            legend=None,
        ),
        tooltip=["level"],
    )
    legend_text = legend_base.mark_text(
        align="center",
        baseline="middle",
        color="white",
    ).encode(
        text="level",
    )
    legend = legend_rects + legend_text
    legend = legend.add_params(comparison_level_filter)
    legend = legend.add_params(vector_ids_filter)
    return legend
