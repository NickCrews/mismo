from __future__ import annotations

import altair as alt
from ibis import _
from ibis.expr import types as ir

from mismo.block import _upset


def upset_chart(blocked: ir.Table) -> alt.Chart:
    """Generate an Altair-based UpSet plot for a blocked table.

    An [UpSet plot](https://en.wikipedia.org/wiki/UpSet_Plot)
    is useful to visualize the overlap between blocking rules.
    For example, how many pairs are blocked by the "first name" rule,
    how many pairs are blocked by the "last name" rule, and how many pairs
    are blocked by both rules together.

    For example, if there is one group that generates a lot of pairs,
    that could be an opportunity to make those rules more restrictive,
    so that they generate fewer pairs.

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
    chart = _upset.upset_chart(intersections)
    total_pairs = intersections.intersection_size.sum().execute()
    title = alt.Title(
        f"{len(rule_names)} Rules generated {total_pairs:,} total pairs",
        anchor="middle",
    )
    chart = chart.properties(title=title)
    return chart
