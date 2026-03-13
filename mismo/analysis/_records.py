"""Per-record analysis for a Linkage.

These functions categorize records by how many links they have:
- unlinked: 0 links (missed/unmatched records)
- singly_linked: exactly 1 link (clean 1-to-1 matches)
- multiply_linked: 2+ links (ambiguous matches)
"""

from __future__ import annotations

from typing import Literal

import ibis
from ibis import _

from mismo.linkage._linkage import Linkage
from mismo.types._linked_table import LinkedTable


def _get_side(linkage: Linkage, side: str) -> LinkedTable:
    if side == "left":
        return linkage.left
    elif side == "right":
        return linkage.right
    raise ValueError(f"side must be 'left' or 'right', got {side!r}")


def with_link_category(
    linkage: Linkage,
    side: Literal["left", "right"] = "left",
    *,
    name: str = "link_category",
) -> LinkedTable:
    """Add a categorical column describing how many links each record has.

    The column has values:
    - "unlinked": 0 links
    - "singly_linked": exactly 1 link
    - "multiply_linked": 2+ links

    Parameters
    ----------
    linkage
        The linkage to analyze.
    side
        "left" or "right".
    name
        Name of the new column.

    Returns
    -------
    The table with an extra column.

    Examples
    --------
    >>> import ibis, mismo
    >>> from mismo import analysis
    >>> ibis.options.interactive = True
    >>> left = ibis.memtable({"record_id": [1, 2, 3]})
    >>> right = ibis.memtable({"record_id": [4, 5]})
    >>> links = ibis.memtable({"record_id_l": [1, 1, 2], "record_id_r": [4, 5, 4]})
    >>> linkage = mismo.Linkage(left=left, right=right, links=links)
    >>> analysis.with_link_category(linkage, "left").execute()
       record_id link_category
    0          1  multiply_linked
    1          2     singly_linked
    2          3          unlinked
    """
    table = _get_side(linkage, side)
    with_n = table.with_n_links()
    n = with_n.n_links
    categorized = with_n.mutate(
        **{
            name: ibis.cases(
                (n == 0, "unlinked"),
                (n == 1, "singly_linked"),
                else_="multiply_linked",
            )
        }
    )
    return categorized


def unlinked(
    linkage: Linkage,
    side: Literal["left", "right"] = "left",
) -> LinkedTable:
    """Records with zero links — those not matched to anything.

    These are the "misses" — records that the linker failed to find a
    match for. Useful for diagnosing gaps in linking coverage.

    Parameters
    ----------
    linkage
        The linkage to analyze.
    side
        "left" or "right".

    Returns
    -------
    A LinkedTable containing only unlinked records.

    Examples
    --------
    >>> import ibis, mismo
    >>> from mismo import analysis
    >>> ibis.options.interactive = True
    >>> left = ibis.memtable({"record_id": [1, 2, 3], "name": ["Alice", "Bob", "Eve"]})
    >>> right = ibis.memtable({"record_id": [4, 5]})
    >>> links = ibis.memtable({"record_id_l": [1], "record_id_r": [4]})
    >>> linkage = mismo.Linkage(left=left, right=right, links=links)
    >>> analysis.unlinked(linkage, "left").execute()
       record_id   name  n_links
    0          2    Bob        0
    1          3    Eve        0
    """
    table = _get_side(linkage, side)
    with_n = table.with_n_links()
    return with_n.filter(_.n_links == 0)


def singly_linked(
    linkage: Linkage,
    side: Literal["left", "right"] = "left",
) -> LinkedTable:
    """Records with exactly one link — clean 1-to-1 matches.

    These are the "clean" matches — each record maps to exactly one
    counterpart. Ideal for deduplication and lookup use cases.

    Parameters
    ----------
    linkage
        The linkage to analyze.
    side
        "left" or "right".

    Returns
    -------
    A LinkedTable containing only singly-linked records.
    """
    table = _get_side(linkage, side)
    with_n = table.with_n_links()
    return with_n.filter(_.n_links == 1)


def multiply_linked(
    linkage: Linkage,
    side: Literal["left", "right"] = "left",
    *,
    min_links: int = 2,
) -> LinkedTable:
    """Records with two or more links — potentially ambiguous matches.

    These records matched multiple counterparts, which may indicate:
    - Duplicates in the other table
    - Over-broad blocking / matching criteria
    - Genuine one-to-many relationships

    Parameters
    ----------
    linkage
        The linkage to analyze.
    side
        "left" or "right".
    min_links
        Minimum number of links to qualify. Default 2.

    Returns
    -------
    A LinkedTable containing only multiply-linked records.
    """
    table = _get_side(linkage, side)
    with_n = table.with_n_links()
    return with_n.filter(_.n_links >= min_links)


def sample_pairs(
    linkage: Linkage,
    n: int = 20,
    *,
    category: Literal["all", "singly_linked", "multiply_linked"] = "all",
    seed: int | None = None,
) -> ibis.Table:
    """Sample linked pairs, showing fields from both sides side-by-side.

    Columns are suffixed with ``_l`` for left and ``_r`` for right.

    Parameters
    ----------
    linkage
        The linkage to analyze.
    n
        Maximum number of pairs to return.
    category
        Filter to a specific link category before sampling.
        - "all": sample from all links
        - "singly_linked": only pairs where both sides have exactly 1 link
        - "multiply_linked": only pairs involving a multiply-linked record
    seed
        Random seed for reproducible sampling.

    Returns
    -------
    An ibis Table of sampled pairs with all fields from both sides.

    Examples
    --------
    >>> analysis.sample_pairs(linkage, n=5).execute()
       record_id_l  name_l  record_id_r  name_r
    0            1   Alice            4     Alce
    """
    links = linkage.links.with_both()
    if category == "singly_linked":
        left_single = linkage.left.with_n_links().filter(_.n_links == 1)
        right_single = linkage.right.with_n_links().filter(_.n_links == 1)
        links = links.filter(
            _.record_id_l.isin(left_single.record_id)
            & _.record_id_r.isin(right_single.record_id)
        )
    elif category == "multiply_linked":
        left_multi = linkage.left.with_n_links().filter(_.n_links >= 2)
        right_multi = linkage.right.with_n_links().filter(_.n_links >= 2)
        links = links.filter(
            _.record_id_l.isin(left_multi.record_id)
            | _.record_id_r.isin(right_multi.record_id)
        )
    if seed is not None:
        sample = links.order_by(ibis.random()).limit(n)
    else:
        sample = links.limit(n)
    return sample
