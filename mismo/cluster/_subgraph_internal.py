from __future__ import annotations

from collections.abc import Iterable, Mapping

import ibis
from ibis import _
import solara

from mismo.cluster import degree


@solara.component
def degree_dashboard(
    tables: ibis.Table | Iterable[ibis.Table] | Mapping[str, ibis.Table],
    links: ibis.Table,
) -> None:
    with_degree = degree(tables, links)

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
