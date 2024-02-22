from __future__ import annotations

import ibis
from ibis.expr import types as it

from mismo import _util


def double_metaphone(s: it.StringValue) -> it.ArrayValue[it.StringValue]:
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
    with _util.optional_import():
        from doublemetaphone import doublemetaphone

    return doublemetaphone(s)
