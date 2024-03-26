from __future__ import annotations

from typing import Sized, Union

from ibis.common.exceptions import ExpressionError
from ibis.expr import types as ir

Sizable = Union[int, Sized, ir.Table]


def _get_len(x: Sizable) -> int:
    if isinstance(x, int):
        return x
    try:
        return len(x)
    except (TypeError, ExpressionError):
        return x.count().execute()


def n_naive_comparisons(left: Sizable, right: Sizable | None = None) -> int:
    """The number of comparisons if we compared every record to every other record.

    Parameters
    ----------
    left : int | Sized | Table
        The number of records in the left dataset, or the left dataset itself.
    right : int | Sized | Table, optional
        The number of records in the right dataset, or the right dataset itself.
        For dedupe tasks leave this as None.
    """
    if right is None:
        return _get_len(left) * (_get_len(left) - 1) // 2
    else:
        return _get_len(left) * _get_len(right)
