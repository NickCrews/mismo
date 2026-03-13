"""Summary statistics for a Linkage."""

from __future__ import annotations

from typing import Any

import ibis
from ibis import _

from mismo.linkage._linkage import Linkage


def summary(linkage: Linkage) -> dict[str, Any]:
    """Compute scalar summary statistics for a Linkage.

    Parameters
    ----------
    linkage
        The linkage to summarize.

    Returns
    -------
    dict with keys:
        - n_left: total records in left table
        - n_right: total records in right table
        - n_links: total links
        - left_linked: records in left with ≥1 link
        - left_unlinked: records in left with 0 links
        - left_multiply_linked: records in left with ≥2 links
        - right_linked: same for right
        - right_unlinked: same for right
        - right_multiply_linked: same for right
        - left_coverage: fraction of left records that are linked
        - right_coverage: fraction of right records that are linked
        - avg_links_per_left: mean number of links per left record
        - avg_links_per_right: mean number of links per right record
        - max_links_per_left: max links on any single left record
        - max_links_per_right: max links on any single right record

    Examples
    --------
    >>> import ibis, mismo
    >>> from mismo import analysis
    >>> ibis.options.interactive = True
    >>> left = ibis.memtable({"record_id": [1, 2, 3]})
    >>> right = ibis.memtable({"record_id": [4, 5, 6, 7]})
    >>> links = ibis.memtable({"record_id_l": [1, 1, 2], "record_id_r": [4, 5, 6]})
    >>> linkage = mismo.Linkage(left=left, right=right, links=links)
    >>> analysis.summary(linkage)
    {'n_left': 3, 'n_right': 4, 'n_links': 3, ...}
    """
    n_left = linkage.left.count().execute()
    n_right = linkage.right.count().execute()
    n_links = linkage.links.count().execute()

    left_with_n = linkage.left.with_n_links()
    right_with_n = linkage.right.with_n_links()

    left_counts = _link_counts_scalar(left_with_n)
    right_counts = _link_counts_scalar(right_with_n)

    def coverage(linked: int, total: int) -> float:
        return round(linked / total, 4) if total > 0 else 0.0

    return {
        "n_left": int(n_left),
        "n_right": int(n_right),
        "n_links": int(n_links),
        "left_linked": left_counts["linked"],
        "left_unlinked": left_counts["unlinked"],
        "left_multiply_linked": left_counts["multiply_linked"],
        "left_coverage": coverage(left_counts["linked"], int(n_left)),
        "left_avg_links": left_counts["avg_links"],
        "left_max_links": left_counts["max_links"],
        "right_linked": right_counts["linked"],
        "right_unlinked": right_counts["unlinked"],
        "right_multiply_linked": right_counts["multiply_linked"],
        "right_coverage": coverage(right_counts["linked"], int(n_right)),
        "right_avg_links": right_counts["avg_links"],
        "right_max_links": right_counts["max_links"],
    }


def _link_counts_scalar(table_with_n: ibis.Table) -> dict[str, Any]:
    """Compute scalar link counts from a table with n_links column."""
    agg = table_with_n.aggregate(
        unlinked=_.n_links.isin([0]).cast("int64").sum(),
        linked=(_.n_links > 0).cast("int64").sum(),
        multiply_linked=(_.n_links > 1).cast("int64").sum(),
        avg_links=_.n_links.mean(),
        max_links=_.n_links.max(),
    ).execute()
    row = agg.iloc[0]
    return {
        "unlinked": int(row["unlinked"]),
        "linked": int(row["linked"]),
        "multiply_linked": int(row["multiply_linked"]),
        "avg_links": round(float(row["avg_links"] or 0), 3),
        "max_links": int(row["max_links"] or 0),
    }


def link_attribute_counts(
    linkage: Linkage, column: str
) -> list[dict[str, Any]]:
    """Count links grouped by a categorical column on the links table.

    Useful when each link has a label like a match level, linker name,
    or blocking key.

    Parameters
    ----------
    linkage
        The linkage to analyze.
    column
        Name of a column on the links table to group by.

    Returns
    -------
    List of dicts with keys: column value, count, fraction.

    Examples
    --------
    >>> links_with_level = ibis.memtable({
    ...     "record_id_l": [1, 2, 3, 4],
    ...     "record_id_r": [5, 6, 7, 8],
    ...     "match_level": ["exact", "exact", "fuzzy", "exact"],
    ... })
    >>> analysis.link_attribute_counts(linkage, "match_level")
    [{'match_level': 'exact', 'count': 3, 'fraction': 0.75},
     {'match_level': 'fuzzy', 'count': 1, 'fraction': 0.25}]
    """
    if column not in linkage.links.columns:
        raise ValueError(
            f"Column {column!r} not found in links table. "
            f"Available columns: {linkage.links.columns}"
        )
    total = linkage.links.count().execute()
    counts = (
        linkage.links.group_by(column)
        .aggregate(count=linkage.links[column].count())
        .order_by(ibis.desc("count"))
        .execute()
    )
    result = []
    for _i, row in counts.iterrows():
        result.append(
            {
                column: row[column],
                "count": int(row["count"]),
                "fraction": round(float(row["count"]) / total, 4)
                if total > 0
                else 0.0,
            }
        )
    return result


def column_stats(
    linkage: Linkage,
    column: str,
    side: str = "left",
) -> dict[str, Any]:
    """Compute statistics for a specific column in left or right table.

    Parameters
    ----------
    linkage
        The linkage to analyze.
    column
        Name of the column to analyze.
    side
        "left" or "right".

    Returns
    -------
    Dict with basic stats: count, null_count, n_distinct, min, max, mean
    (mean/min/max only for numeric/date columns).
    """
    table = linkage.left if side == "left" else linkage.right
    if column not in table.columns:
        raise ValueError(
            f"Column {column!r} not found in {side} table. "
            f"Available columns: {table.columns}"
        )
    col = table[column]
    aggs: dict[str, Any] = {
        "count": col.count(),
        "null_count": col.isnull().cast("int64").sum(),
        "n_distinct": col.nunique(),
    }
    dtype = table.schema()[column]
    if dtype.is_numeric() or dtype.is_temporal():
        aggs["min"] = col.min()
        aggs["max"] = col.max()
    if dtype.is_numeric():
        aggs["mean"] = col.mean()
    result = table.aggregate(**aggs).execute().iloc[0].to_dict()
    return {k: (v.item() if hasattr(v, "item") else v) for k, v in result.items()}
