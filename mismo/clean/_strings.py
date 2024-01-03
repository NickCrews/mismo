from __future__ import annotations

from ibis.expr.types import StringValue


def norm_whitespace(texts: StringValue) -> StringValue:
    """
    Strip leading/trailing whitespace, replace multiple whitespace with a single space.
    """
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore
