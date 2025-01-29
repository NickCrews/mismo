from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util


def norm_whitespace(texts: ir.StringValue) -> ir.StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    texts = _util.ensure_ibis(texts, "string")
    return texts.strip().re_replace(r"\s+", " ")


def strip_accents(s: ir.StringValue) -> ir.StringValue:
    """Remove accents, such as é -> e. Only works with duckdb.

    Parameters
    ----------
    s
        The string to strip

    Returns
    -------
    ir.StringValue
        The string with non-ascii characters replaced and/or removed.

    Examples
    --------
    >>> import ibis
    >>> strip_accents(ibis.literal("müller")).execute()
    'muller'
    >>> strip_accents(ibis.literal("François")).execute()
    'Francois'
    >>> strip_accents(ibis.literal("Øslo")).execute()  # Ø is not an accent
    'Øslo'
    >>> strip_accents(ibis.literal("æ")).execute()  # neither is this
    'æ'
    >>> strip_accents(ibis.literal("ɑɽⱤoW")).execute()  # neither is this
    'ɑɽⱤoW'
    """
    # There is a python lib unidecode, but I don't want us to take the performance hit.
    # polars also appears to implement this.
    # But there is no pyarrow implementation :(
    is_duckdb = (
        isinstance(s, ibis.Deferred)
        or s._find_backend(use_default=True).name == "duckdb"
    )
    if not is_duckdb:
        raise NotImplementedError("remove_accents not implemented for non-duckdb")
    s = _strip_accents(s)
    return s


@ibis.udf.scalar.builtin(name="strip_accents", signature=(("string",), "string"))
def _strip_accents(s: ir.StringValue) -> ir.StringValue: ...
