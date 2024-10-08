from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util


def double_metaphone(s: ir.StringValue) -> ir.ArrayValue[ir.StringValue]:
    """Double Metaphone phonetic encoding

    This requires the [doublemetaphone](https://github.com/dedupeio/doublemetaphone)
    package to be installed.
    You can install it with `python -m pip install DoubleMetaphone`.
    This uses a python UDF so it is going to be slow.

    Examples
    --------
    >>> from mismo.text import double_metaphone
    >>> double_metaphone("catherine").execute()
    ['K0RN', 'KTRN']
    >>> double_metaphone("").execute()
    ['', '']
    >>> double_metaphone(None).execute() is None
    True
    """
    s = _util.ensure_ibis(s, "string")
    return _dm_udf(s)


@ibis.udf.scalar.python(signature=(("string",), "array<string>"))
def _dm_udf(s):
    with _util.optional_import("DoubleMetaphone"):
        from doublemetaphone import doublemetaphone

    return doublemetaphone(s)


# TODO: this isn't portable between backends
@ibis.udf.scalar.builtin
def damerau_levenshtein(a: str, b: str) -> int:
    """
    The number of adds, deletes, substitutions, and transposes to get from `a` to `b`.

    This is the levenstein distance with the addition of transpositions as
    a possible operation.
    """


def levenshtein_ratio(s1: ir.StringValue, s2: ir.StringValue) -> ir.FloatingValue:
    """The levenshtein distance between two strings, normalized to be between 0 and 1.

    The ratio is defined as `(lenmax - ldist)/lenmax` where

    - `ldist` is the regular levenshtein distance
    - `lenmax` is the maximum length of the two strings
      (eg the largest possible edit distance)

    This makes it so that the ratio is 1 when the strings are the same and 0
    when they are completely different.
    By doing this normalization, the ratio is always between 0 and 1, regardless
    of the length of the strings.

    Parameters
    ----------
    s1:
        The first string

    s2:
        The second string

    Returns
    -------
    lev_ratio:
        The ratio of the Levenshtein edit cost to the maximum string length

    Examples
    --------
    >>> from mismo.text import levenshtein_ratio
    >>> levenshtein_ratio("mile", "mike").execute()
    np.float64(0.75)
    >>> levenshtein_ratio("mile", "mile").execute()
    np.float64(1.0)
    >>> levenshtein_ratio("mile", "").execute()
    np.float64(0.0)
    >>> levenshtein_ratio("", "").execute()
    np.float64(nan)
    """
    return _dist_ratio(s1, s2, lambda a, b: a.levenshtein(b))


def damerau_levenshtein_ratio(
    s1: ir.StringValue, s2: ir.StringValue
) -> ir.FloatingValue:
    """Like levenshtein_ratio, but with the Damerau-Levenshtein distance.

    See Also
    --------
    - [damerau_levenshtein()][mismo.text.damerau_levenshtein]
    - [levenshtein_ratio()][mismo.text.levenshtein_ratio]
    """
    return _dist_ratio(s1, s2, damerau_levenshtein)


def _dist_ratio(s1, s2, dist):
    s1 = _util.ensure_ibis(s1, "string")
    s2 = _util.ensure_ibis(s2, "string")
    lenmax = ibis.greatest(s1.length(), s2.length())
    return (lenmax - dist(s1, s2)) / lenmax
