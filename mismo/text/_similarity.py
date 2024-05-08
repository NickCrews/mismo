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
    >>> from mismo.clean import double_metaphone
    >>> double_metaphone("catherine").execute()
    ['K0RN', 'KTRN']
    >>> double_metaphone("").execute()
    ['', '']
    >>> double_metaphone(None).execute()
    None
    """
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
