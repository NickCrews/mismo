from __future__ import annotations

import altair as alt
import ibis

import typing

if typing.TYPE_CHECKING:
    from ._weights import Weights


def plot_weights(weights: Weights) -> alt.Chart:
    t = _weights_to_table(weights)
    ms = _subplot(
        t,
        alt.X(
            "m",
            title=["Proportion of Pairs", "Amongst Matches"],
            axis=alt.Axis(format="%"),
        ),
        "green",
        True,
    )
    us = _subplot(
        t,
        alt.X(
            "u",
            title=["Proportion of Pairs", "Amongst Non-Matches"],
            axis=alt.Axis(format="%"),
        ),
        "red",
        False,
    )
    odds = _subplot(
        t,
        alt.X("log_odds", title="Log Odds"),
        # alt.X("odds", title="Log Odds", scale=alt.Scale(type="log")),
        # alt.Color(
        # "log_odds",
        # scale=alt.Scale(domain=[-5, 0, 5], range=["red", "grey", "green"]),
        # ),
        # Ideally can fix this once https://github.com/vega/vega-lite/issues/9139
        # is fixed
        "black",
        False,
    )
    together = alt.hconcat(
        ms,
        us,
        odds,
        spacing=10,
    )
    together = together.properties(title=alt.Title(text="Weights", anchor="middle"))
    return together


def _weights_to_table(weights):
    records = []
    for comparison_weights in weights:
        level_weights = list(comparison_weights.level_weights) + [
            comparison_weights.else_weights
        ]
        for i, lw in enumerate(level_weights):
            records.append(
                {
                    "comparison": comparison_weights.name,
                    "level": lw.name,
                    "m": lw.m,
                    "u": lw.u,
                    "odds": lw.odds,
                    "log_odds": lw.log_odds,
                    "level_order": i,
                }
            )

    return ibis.memtable(records)


def _subplot(t, x, color, use_y_axis):
    if use_y_axis:
        y = alt.Y("level", title=None, sort=alt.EncodingSortField("level_order"))
        row = alt.Row("comparison", title=None, header=alt.Header(orient="left"))
    else:
        y = alt.Y(
            "level", title=None, axis=None, sort=alt.EncodingSortField("level_order")
        )
        row = alt.Row("comparison", title=None, header=None)
    chart = (
        alt.Chart(
            t,
            width=150,
        )
        .encode(
            x=x,
            y=y,
            row=row,
            tooltip=["comparison", "level", "m", "u", "odds", "log_odds"],
        )
        .resolve_scale(y="independent")
    )
    if isinstance(color, str):
        chart = chart.mark_bar(color=color)
    else:
        chart = chart.mark_bar().encode(color=color)
    return chart
