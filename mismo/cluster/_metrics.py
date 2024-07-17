from __future__ import annotations

from typing import Iterable, Mapping, overload

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo._datasets import Datasets


@overload
def degree(
    *,
    links: ir.Table,
    records: ir.Table | None,
) -> ir.Table: ...


@overload
def degree(
    *,
    links: ir.Table,
    records: Iterable[ir.Table] | Mapping[str, ir.Table],
) -> Datasets: ...


def degree(
    *,
    links: ir.Table,
    records: ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table] | None = None,
) -> ir.Table | Datasets:
    """Label records with their degree (number of links to other records).

    This is the graph theory definition of degree, i.e. the number of vertices
    coming into or out of a vertex. In this case, the number of links coming
    into or out of a record.

    Parameters
    ----------
    links :
        A table of edges with at least columns (record_id_l, record_id_r).
    records :
        Table(s) of records with at least the column `record_id`, or None.

    Returns
    -------
    result
        If `records` is None, a Table will be returned with columns
        `record_id` and `degree:uint64` that maps record_id to a degree.
        If `records` is a single Table, that table will be returned
        with a `degree:uint64` column added.
        If an iterable/mapping of Tables is given, a `Datasets` will be returned,
        with a `component` column added to each contained Table.
    """
    l1 = links.select(record_id="record_id_l", other="record_id_r")
    l2 = links.select(record_id="record_id_r", other="record_id_l")
    u = ibis.union(l1, l2, distinct=True)
    lookup = u.group_by("record_id").agg(degree=_.count())
    if records is None:
        return lookup
    ds = Datasets(records).map(
        lambda _name, t: t.left_join(lookup, "record_id")
        .mutate(degree=_.degree.fill_null(0))
        .drop("record_id_right")
    )
    if len(ds) == 1:
        return ds[0]
    return ds
