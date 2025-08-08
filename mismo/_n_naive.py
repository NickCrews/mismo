from __future__ import annotations

from typing import Sized

from ibis.common.exceptions import ExpressionError
from ibis.expr import types as ir


def n_naive_comparisons(
    left: ir.Table | Sized | int, right: ir.Table | Sized | int | None = None
) -> int:
    """The number of comparisons if we compared every record to every other record.

    Parameters
    ----------
    left
        The number of records in the left dataset, or the left dataset itself.
    right
        The number of records in the right dataset, or the right dataset itself.
        For dedupe tasks leave this as None.

    Returns
    -------
    int
        The number of comparisons.
    """
    if right is None:
        # dedupe
        return _get_len(left) * (_get_len(left) - 1) // 2
    else:
        # link
        return _get_len(left) * _get_len(right)


def _get_len(x: ir.Table | Sized | int) -> int:
    if isinstance(x, int):
        return x
    try:
        return len(x)
    except (TypeError, ExpressionError):
        return x.count().execute()
