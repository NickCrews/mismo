from __future__ import annotations

import ibis
from ibis.expr import types as ir


def norm_whitespace(texts: ir.StringValue) -> ir.StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore


@ibis.udf.scalar.builtin(
    name="regexp_extract_all", signature=(("string", "string"), "array<string>")
)
def _re_extract_all(string, pattern): ...

def _to_ibis_type(string: ir.StringValue) -> ir.StringValue:
    if not isinstance(string, ir.Expr) and not isinstance(string, ibis.Deferred):
        string = ibis.literal(string, type="string")
    return string

# from https://www.imperva.com/blog/fast-n-grams-extraction-and-analysis-with-sql/
def ngrams(string: ir.StringValue, n: int) -> ir.ArrayValue:
    """
    Character n-grams from a string. The order of the n-grams is not guaranteed.

    Parameters
    ----------
    string:
        The string to generate n-grams from.
    n:
        The number of characters in each n-gram.

    Returns
    -------
    An array of n-grams.

    Examples
    --------
    >>> import ibis
    >>> from mismo.text import ngrams
    >>> ngrams("abc", 2).execute()
    ["ab", "bc"]
    >>> ngrams("", 2).execute()
    []
    >>> ngrams("a", 2).execute()
    []
    >>> ngrams(None, 4).execute()
    None

    Order of n-grams is not guaranteed:

    >>> ngrams("abcdef", 3).execute()
    ["abc", "def", "bcd", "cde"]
    """
    string = _to_ibis_type(string)
    pattern = "." * n
    # if you just do _re_extract_all("abcdef", "..."), you get ["abc", "def"].
    # So to get the "bcd" and the "cde", we need to offset the string
    # by one and two (in general up to n-1) characters.
    result: ir.ArrayValue = None
    for i in range(0, n):
        this = _re_extract_all(string[i:], pattern)
        if result is None:
            result = this
        else:
            result = result.concat(this)
    return result


def levenshtein_ratio(s1: ir.StringValue, s2: ir.StringValue) -> ir.FloatingValue:
    """Uses the Levenshtein distance to calculate the similarity between two strings.

    This is similar to the Levenshtein distance, but it is normalized to be between 0 and 1 
    and is resilient to different string lengths.

    The ratio is defined as `(lensum - ldist)/lensum` where `lensum` is the
    maximum length of the two strings and ldist is the number of edits required
    to transform one string into the other.

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
    s1 = _to_ibis_type(s1)
    s2 = _to_ibis_type(s2)
    lensum = ibis.greatest(s1.length(), s2.length())
    ldist = s1.levenshtein(s2)
    return (lensum - ldist) / lensum
