from __future__ import annotations

from ibis.expr import types as ir
import ibis


def levenshtein_ratio(s1: ir.StringColumn, s2: ir.StringColumn) -> ir.FloatingColumn:
    """Computes the similarity ratio between two strings
    using the Levenshtein distance.

    This is defined as `(lensum - ldist)/lensum` where `lensum` is the
    maximum length of the two strings and ldist is the number of edits required 
    to transform one string into the other

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
    lensum = ibis.greatest(s1.length(), s2.length())
    ldist = s1.levenshtein(s2)
    return (lensum - ldist)/lensum
