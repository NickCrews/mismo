from __future__ import annotations

from ibis.expr import types as ir


def levenshtein_ratio(s1: ir.StringColumn, s2: ir.StringColumn) -> ir.FloatingColumn:
    """Computes the similarity ratio between two strings
    using the Levenshtein distance.

    This is defined as `(1 - lcost/lsum)` where `lcost` is the
    cost of replacing characters in one string to match the other
    and `lsum` is the total length of the two strings

    Parameters
    ----------
    s1:
        The first string

    s2:
        The second string

    Returns
    -------
    lev_ratio:
        The ratio of the Levenshtein edit cost to the total string length
    """

    lev_cost = 2 * s1.levenshtein(s2)
    len_sum = s1.length() + s2.length()
    return 1 - lev_cost / len_sum
