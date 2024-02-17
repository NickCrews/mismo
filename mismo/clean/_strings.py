from __future__ import annotations

from ibis.expr import types as it


def norm_whitespace(texts: it.StringValue) -> it.StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore
