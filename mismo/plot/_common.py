from __future__ import annotations

import altair as alt

LOG_ODDS_COLOR_SCALE = alt.Scale(
    domainMid=0,
    domainMin=-3,
    domainMax=3,
    scheme="redyellowgreen",
)
