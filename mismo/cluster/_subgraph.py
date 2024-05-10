from __future__ import annotations

from typing import Iterable, Mapping

from ibis import _
from ibis.expr import types as ir
import solara

from mismo.cluster import add_degree


@solara.component
def degree_dashboard(
    tables: ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
):
    """Make a dashboard for exploring the degree (number of links) of records.

    The "degree" of a record is the number of other records it is linked to.

    Pass the entire dataset and the links between records,
    and use this to explore the distribution of degrees.
    """
    with_degree = add_degree(tables, links)

    def degree_hist(links_by_record):
        return (
            links_by_record.group_by(_.degree.name("Degree"))
            .agg(_.count().name("Number of Records"))
            .order_by("Degree")
        )

    solara.Markdown(
        """
        ### Distribution of Degrees

        The "degree" of a record is the number of other records it is linked to. Use this to find how many records
        
        - were not linked
        - were linked to just one other record
        - were linked to many other records
        """,  # noqa: E501
        style="align: center",
    )
    with solara.Columns():
        for name, table in with_degree.items():
            with solara.Column():
                solara.Markdown(f"### {name}", style="text-align: center")
                solara.DataFrame(degree_hist(table).execute())
