from __future__ import annotations

from ibis.expr import types as ir

from mismo import _util


def norm_whitespace(texts: ir.StringValue) -> ir.StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    texts = _util.ensure_ibis(texts, "string")
    return texts.strip().re_replace(r"\s+", " ")
