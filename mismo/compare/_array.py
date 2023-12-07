from __future__ import annotations

from ibis.expr.types import ArrayValue, FloatingValue


def jaccard(a: ArrayValue, b: ArrayValue) -> FloatingValue:
    """The Jaccard similarity between two arrays.

    Parameters
    ----------
    a : ArrayValue
        The first array.
    b : ArrayValue
        The second array.

    Returns
    -------
    FloatingValue
        The Jaccard similarity between the two arrays.
    """
    intersection = a.intersect(b).length()
    normal = intersection / a.union(b).length()
    return (intersection == 0).ifelse(0, normal)
