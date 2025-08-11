from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

import ibis

if TYPE_CHECKING:
    import solara


def degree_dashboard(
    tables: ibis.Table | Iterable[ibis.Table] | Mapping[str, ibis.Table],
    links: ibis.Table,
) -> solara.Componenta:
    """Make a dashboard for exploring the degree (number of links) of records.

    The "degree" of a record is the number of other records it is linked to.

    Pass the entire dataset and the links between records,
    and use this to explore the distribution of degrees.
    """
    from mismo.cluster._subgraph_internal import degree_dashboard

    return degree_dashboard(tables, links)
