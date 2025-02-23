from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util


def tokenize(text: ir.StringValue) -> ir.ArrayValue:
    """
    Split a string into tokens on whitespace.

    Examples
    --------
    >>> import ibis
    >>> from mismo.text import tokenize
    >>> tokenize(ibis.literal("  abc    def")).execute()
    ['abc', 'def']
    >>> tokenize(ibis.literal("  abc")).execute()
    ['abc']
    >>> tokenize(ibis.literal(" ")).execute()
    []
    >>> tokenize(ibis.null(str)).execute() is None
    True
    """
    stripped = text.strip()
    return (stripped == "").ifelse([], stripped.re_split(r"\s+"))


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
    ['ab', 'bc']
    >>> ngrams("", 2).execute()
    []
    >>> ngrams("a", 2).execute()
    []
    >>> ngrams(None, 4).execute() is None
    True

    Order of n-grams is not guaranteed:

    >>> ngrams("abcdef", 3).execute()
    ['abc', 'def', 'bcd', 'cde']
    """
    if n < 1:
        raise ValueError("n must be greater than 0")
    string = _util.ensure_ibis(string, "string")
    pattern = "." * n
    # if you just do _re_extract_all("abcdef", "..."), you get ["abc", "def"].
    # So to get the "bcd" and the "cde", we need to offset the string
    # by one and two (in general up to n-1) characters.
    first, *rest = [_re_extract_all(string[i:], pattern) for i in range(0, n)]
    return string.isnull().ifelse(ibis.null("array<string>"), first.concat(*rest))


@ibis.udf.scalar.builtin(
    name="regexp_extract_all", signature=(("string", "string"), "array<string>")
)
def _re_extract_all(string, pattern): ...
