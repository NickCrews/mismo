from __future__ import annotations

import ibis
from ibis.common.deferred import Deferred
from ibis.expr.types import BooleanColumn, Table

from mismo import _util


def join(
    left: Table,
    right: Table,
    predicates=tuple(),
    how="inner",
    *,
    lname: str = "",
    rname: str = "{name}_right",
) -> Table:
    """Similar to ibis's join, with a few differences

    - Does a cross join when predicates is True or how is "cross"
    - Converts the lname and rname suffixes to the appropriate kwargs for ibis<6.0.0
    - Allows for a wider set of join predicates:
      - anything that ibis accepts as a join predicate
      - tuple[str, str]
      - tuple[Column, Column]
      - tuple[Deferred, Deferred]
      - lambda (left, right) -> any of the above
    """
    rename_kwargs = _join_suffix_kwargs(lname=lname, rname=rname)
    preds = resolve_predicates(left, right, predicates)
    return left.join(right, predicates=preds, how=how, **rename_kwargs)


def resolve_predicates(
    left: Table, right: Table, raw, **kwargs
) -> list[bool | BooleanColumn | Table]:
    """Resolve the predicates for a join"""
    if isinstance(raw, tuple):
        if len(raw) != 2:
            raise ValueError(f"predicates must be a tuple of length 2, got {raw}")
        raw = [raw]
    # Deferreds are callable, so we have to guard against them
    if callable(raw) and not isinstance(raw, Deferred):
        return resolve_predicates(left, right, raw(left, right, **kwargs))
    preds = _util.promote_list(raw)
    return [_resolve_predicate(left, right, pred) for pred in preds]


def _resolve_predicate(left: Table, right: Table, raw) -> bool | BooleanColumn | Table:
    if isinstance(raw, Table):
        return raw
    if isinstance(raw, BooleanColumn):
        return raw
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, tuple):
        if len(raw) != 2:
            raise ValueError(f"predicate must be a tuple of length 2, got {raw}")
        return _util.get_column(left, raw[0]) == _util.get_column(right, raw[1])
    if isinstance(raw, Deferred):
        return _util.get_column(left, raw) == _util.get_column(right, raw)
    if isinstance(raw, str):
        return _util.get_column(left, raw) == _util.get_column(right, raw)
    # This case must come after the Deferred case, because Deferred is callable
    if callable(raw):
        return _resolve_predicate(left, right, raw(left, right))


def _join_suffix_kwargs(lname: str, rname: str) -> dict:
    """create the suffix kwargs for ibis.join(), no matter the ibis version.

    The suffixes kwarg got split into lname and rname in ibis 6.0.0:
    https://ibis-project.org/release_notes/#600-2023-07-05"""
    if ibis.__version__ >= "6.0.0":
        return {"lname": lname, "rname": rname}
    else:

        def _convert_suffix(suffix: str) -> str:
            if not len(suffix):
                return ""
            if not suffix.startswith("{name}"):
                raise ValueError(
                    "suffix must be empty or start with '{name}'"
                    f"for ibis<6.0.0, got {suffix}"
                )
            return suffix.removeprefix("{name}")

        lsuffix = _convert_suffix(lname)
        rsuffix = _convert_suffix(rname)
        return {"suffixes": (lsuffix, rsuffix)}
