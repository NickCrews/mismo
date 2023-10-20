from __future__ import annotations

import altair as alt
from ibis import _
from ibis.expr.types import Table

from mismo.block import _upset


def upset_plot(blocked: Table) -> alt.Chart:
    """Generate an Altair-based UpSet plot for a blocked table.

    Parameters
    ----------
    blocked
        The blocked table to plot.

    Returns
    -------
    Chart
        An Altair chart.
    """
    intersections = blocked.group_by("blocking_rules").agg(intersection_size=_.count())
    intersections = intersections.cache()
    rule_names: list[str] = (
        intersections.blocking_rules.unnest()
        .as_table()
        .distinct()
        .blocking_rules.execute()
        .tolist()
    )
    m = {name: _.blocking_rules.contains(name) for name in rule_names}
    intersections = intersections.mutate(**m)
    intersections = intersections.drop("blocking_rules")
    return _upset.upset_plot(intersections)
