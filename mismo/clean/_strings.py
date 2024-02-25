from __future__ import annotations

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it


def norm_whitespace(texts: it.StringValue) -> it.StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore


@ibis.udf.scalar.builtin(
    name="regexp_extract_all", signature=(("string", "string"), "array<string>")
)
def _re_extract_all(string, pattern):
    ...


# from https://www.imperva.com/blog/fast-n-grams-extraction-and-analysis-with-sql/
def ngrams(string: it.StringValue, n: int) -> it.ArrayValue:
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
    >>> from mismo.clean import ngrams
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
    if not isinstance(string, it.Expr) and not isinstance(string, Deferred):
        string = ibis.literal(string, type="string")
    pattern = "." * n
    # if you just do _re_extract_all("abcdef", "..."), you get ["abc", "def"].
    # So to get the "bcd" and the "cde", we need to offset the string
    # by one and two (in general up to n-1) characters.
    result: it.ArrayValue = None
    for i in range(0, n):
        this = _re_extract_all(string[i:], pattern)
        if result is None:
            result = this
        else:
            result = result.concat(this)
    return result
