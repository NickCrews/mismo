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
    >>> double_metaphone(None).execute()
    None
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
    0.75
    >>> levenshtein_ratio("mile", "mile").execute()
    1.0
    >>> levenshtein_ratio("mile", "").execute()
    0.0
    >>> levenshtein_ratio("", "").execute()
    nan
    """
    s1 = _util.ensure_ibis(s1, "string")
    s2 = _util.ensure_ibis(s2, "string")
    lenmax = ibis.greatest(s1.length(), s2.length())
    ldist = s1.levenshtein(s2)
    return (lenmax - ldist) / lenmax

def token_set_ratio(s1: ir.StringValue, s2: ir.StringValue) -> ir.FloatingValue:
    """The ratio of the intersection of the token sets of two strings to the union of the token sets.

    This is a measure of how similar two strings are, based on the set of tokens they contain.
    It is a variation of the Jaccard index, where the intersection and union are based on the
    set of tokens in the strings.

    Parameters
    ----------
    s1:
        The first string

    s2:
        The second string

    Returns
    -------
    token_set_ratio:
        The ratio of the intersection of the token sets to the union of the token sets

    Examples
    --------
    >>> from mismo.text import token_set_ratio
    >>> token_set_ratio("mile mile", "mile mike").execute()
    0.75
    >>> token_set_ratio("mile mile", "mile").execute()
    1.0
    >>> token_set_ratio("mile mile", "").execute()
    0.0
    >>> token_set_ratio("", "").execute()
    nan
    """
    s1 = _util.ensure_ibis(s1, "string")
    s2 = _util.ensure_ibis(s2, "string")

    # Extract unique tokens from the strings
    tokens1 = _util.tokenize(s1, unique=True, remove_punctuation=True)
    tokens2 = _util.tokenize(s2, unique=True, remove_punctuation=True)
    
    
    # Find the intersection and differences
    intersection = tokens1.intersect(tokens2)
    difference1 = tokens1.filter(lambda x: ~tokens2.contains(x))
    difference2 = tokens2.filter(lambda x: ~tokens1.contains(x))
    
    # Calculate lengths
    len_intersection = intersection.length()
    len_diff1 = difference1.length()
    len_diff2 = difference2.length()
    
    # Calculate scores
    score1 = len_intersection / (len_intersection + len_diff1)
    score2 = len_intersection / (len_intersection + len_diff2)
    score3 = (len_intersection + len_diff1) / (len_intersection + len_diff2)
    
    # Calculate final ratio
    ratio = ibis.greatest(score1, score2, score3) * 100
    return ratio


def token_sort_ratio(s1: ir.StringValue, s2: ir.StringValue) -> ir.FloatingValue:
    """The levenshtein ratio of two strings after tokenizing and sorting the tokens.

    This is a useful measure of similarity when the order of the tokens is not important, 
    for example with addresses.

    Parameters
    ----------
    s1:
        The first string

    s2:
        The second string

    Returns
    -------
    token_sort_ratio:
        The levenstein ratio of the sorted tokens

    Examples
    --------
    >>> from mismo.text import token_sort_ratio
    >>> token_sort_ratio("mile mile", "mile mike").execute()
    0.75
    >>> token_sort_ratio("mile mile", "mile").execute()
    1.0
    >>> token_sort_ratio("mile mile", "").execute()
    0.0
    >>> token_sort_ratio("", "").execute()
    nan
    """
    s1 = _util.ensure_ibis(s1, "string")
    s2 = _util.ensure_ibis(s2, "string")

    tokens1 = _util.tokenize(s1, remove_punctuation=True)
    tokens2 = _util.tokenize(s2, remove_punctuation=True)
    
    sorted_tokens1 = tokens1.sort()
    sorted_tokens2 = tokens2.sort()
    
    sorted_str1 = sorted_tokens1.join(' ')
    sorted_str2 = sorted_tokens2.join(' ')
    
    ratio = levenshtein_ratio(sorted_str1, sorted_str2)
    return ratio * 100


def partial_token_sort_ratio(s1: ir.StringValue, s2: ir.StringValue) -> ir.FloatingValue:
    """Similar to token_sort_ratio, but only uses the minimum length string
    
    This is useful when one of the strings may contain additional noise

    """
    s1 = _util.ensure_ibis(s1, "string")
    s2 = _util.ensure_ibis(s2, "string")

    tokens1 = _util.tokenize(s1, remove_punctuation=True)
    tokens2 = _util.tokenize(s2, remove_punctuation=True)
    
    sorted_tokens1 = tokens1.sort()
    sorted_tokens2 = tokens2.sort()
    
    sorted_str1 = sorted_tokens1.join(' ')
    sorted_str2 = sorted_tokens2.join(' ')

    min_len = ibis.least(sorted_str1.length(), sorted_str2.length())
    sorted_str1 = sorted_str1.left(min_len)
    sorted_str2 = sorted_str2.left(min_len)
    
    ratio = levenshtein_ratio(sorted_str1, sorted_str2)
    return ratio * 100