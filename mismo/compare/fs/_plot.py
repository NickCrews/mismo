from __future__ import annotations

from typing import Iterable

import altair as alt
import pandas as pd

from ._weights import ComparisonWeights


def plot_weights(weights: ComparisonWeights | Iterable[ComparisonWeights]) -> alt.Chart:
    """Plot the weights for a comparison or comparisons.

    Use this to
    - See which levels are common and which are rare.
      If all pairs are getting matched by only one level, you probably
      want to adjust the conditions so that pairs are more evenly distributed.
      For example, if you have an "exact match" level that hardly is ever used,
      that could be an indication that your condition is too strict and you should
      relax it.
    - See the odds for each level.
      If the odds for a "exact match" level are lower than you expect, perhaps
      near 1, that could be an indication that your condition is too loose and
      there are many non-matches sneaking into that level. You should inspect
      those pairs and figure out how to tighted the condition so
      that only matches are in that level.

    Parameters
    ----------
    weights : ComparisonWeights | Iterable[ComparisonWeights]
        The weights to plot.

    Returns
    -------
    alt.Chart
        The plot.
    """
    if isinstance(weights, ComparisonWeights):
        weights = [weights]
    subplots = [_plot_comparison_weights(cw) for cw in weights]
    together = alt.vconcat(*subplots, spacing=30)
    return together.interactive()


def _plot_comparison_weights(cw: ComparisonWeights) -> alt.Chart:
    t = _comp_weights_to_table(cw)
    mu_width = 200
    ms = _subplot(
        t,
        alt.X(
            "m",
            title=["Proportion of Pairs", "Amongst Matches"],
            axis=alt.Axis(format="%"),
            scale=alt.Scale(domain=[0, 1]),
        ),
        alt.value("green"),
        True,
        mu_width,
    )
    us = _subplot(
        t,
        alt.X(
            "u",
            title=["Proportion of Pairs", "Amongst Non-Matches"],
            axis=alt.Axis(format="%"),
            scale=alt.Scale(domain=[0, 1]),
        ),
        alt.value("red"),
        False,
        mu_width,
    )
    odds = _subplot(
        t,
        alt.X(
            "log_odds",
            title="Odds",
            scale=alt.Scale(domain=[-3, 3]),
            axis=alt.Axis(labelExpr=10**alt.datum.value),
        ),
        alt.Color(
            "log_odds",
            title="Log Odds",
            scale=alt.Scale(
                range=["red", "grey", "green"],
                domainMid=0,
            ),
            legend=None,
        ),
        False,
        width=250,
    )
    together = alt.hconcat(ms, us, odds, spacing=5)
    together = together.properties(
        title=alt.Title(text=f"Weights for Comparison '{cw.name}'", anchor="middle")
    )
    return together


def _comp_weights_to_table(comparison_weights: ComparisonWeights) -> pd.DataFrame:
    records = []
    for i, lw in enumerate(comparison_weights):
        records.append(
            {
                # "comparison": comparison_weights.name,
                "level": lw.name,
                "m": lw.m,
                "u": lw.u,
                "odds": lw.odds,
                "log_odds": lw.log_odds,
                "level_order": i,
            }
        )
    return pd.DataFrame(records)


def _subplot(t, x, color, use_y_axis, width):
    axis = alt.Axis() if use_y_axis else None
    y = alt.Y("level", title=None, sort=alt.EncodingSortField("level_order"), axis=axis)
    t = t.copy()
    t["explanation"] = _odds_explanation(t["odds"])
    chart = (
        alt.Chart(t, width=width)
        .encode(
            x=x,
            y=y,
            tooltip=[
                alt.Tooltip("level", title="Level"),
                alt.Tooltip("m", title="Proportion Amongst Matching Pairs", format="%"),
                alt.Tooltip(
                    "u", title="Proportion Amongst Non-Matching Pairs", format="%"
                ),
                alt.Tooltip("odds", title="Odds", format="f"),
                alt.Tooltip("explanation", title="Explanation"),
            ],
            color=color,
        )
        .mark_bar()
    )
    return chart


def _odds_explanation(odds):
    def format_odds(odds):
        return odds.apply(lambda float_: f"{float_:,.3f}")

    more = (
        "Seeing this level makes the odds of a match "
        + format_odds(odds)
        + " times more likely"
    )
    less = (
        "Seeing this level makes the odds of a match "
        + format_odds(1 / odds)
        + " times less likely"
    )
    return more.where((odds >= 1), less)
