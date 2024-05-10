from __future__ import annotations

from typing import Iterable, Mapping

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo._datasets import Datasets


# TODO: need to change this API to be more generic so it can deal with
# dedupe (only one table) as well as linkage (left and right) (and other numbers??)
def add_degree(
    tables: ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table],
    links: ir.Table,
) -> Datasets:
    """Add the degree of each record in the left and/or right tables.

    This is the graph theory definition of degree, i.e. the number of vertices
    coming into or out of a vertex. In this case, the number of links coming
    into or out of a record.

    Parameters
    ----------
    tables :
        table(s) of records with at least the column `record_id`.
    links :
        A table of edges with at least columns (record_id_l, record_id_r)

    Returns
    -------
    A new Datasets with the degree of each record in the left and/or right tables.
    """
    ds = Datasets(tables)
    l1 = links.select(record_id="record_id_l", other="record_id_r")
    l2 = links.select(record_id="record_id_r", other="record_id_l")
    u = ibis.union(l1, l2, distinct=True)
    lookup = u.group_by("record_id").agg(degree=_.count())
    return ds.map(
        lambda name, t: t.left_join(lookup, "record_id").mutate(
            degree=_.degree.fillna(0)
        )
    )
