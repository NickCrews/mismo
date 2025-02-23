from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util


def _resolve_column(spec, t: ir.Table) -> ir.Column:
    values = t.bind(spec)
    if len(values) != 1:
        raise ValueError(f"Expected 1 column, got {len(values)} from {spec}")
    return values[0]


def resolve_column_pair(
    spec, left: ir.Table, right: ir.Table
) -> tuple[ir.Column, ir.Column]:
    if isinstance(spec, (str, ibis.Deferred)):
        return left[spec], right[spec]
    if isinstance(spec, tuple):
        if len(spec) != 2:
            raise ValueError(
                f"Column spec, when a tuple, must be of form (left, right), got {spec}"
            )
        return (_resolve_column(spec[0], left), _resolve_column(spec[1], right))
    try:
        return resolve_column_pair(spec(left, right), left, right)
    except TypeError:
        pass
    lcol = spec(left)
    rcol = spec(right)
    return resolve_column_pair((lcol, rcol), left, right)


def resolve_condition(spec, left: ir.Table, right: ir.Table) -> ir.BooleanValue | bool:
    if isinstance(spec, (ir.BooleanValue, bool)):
        return spec
    if isinstance(spec, (str, ibis.Deferred)):
        return left[spec] == right[spec]
    if isinstance(spec, tuple):
        if len(spec) != 2:
            raise ValueError(
                f"Tuple join conditions must be of form (left, right), got {spec}"
            )
        return _resolve_column(spec[0], left) == _resolve_column(spec[1], right)
    try:
        return resolve_condition(spec(left, right), left, right)
    except TypeError:
        pass
    lcol = spec(left)
    rcol = spec(right)
    return resolve_condition((lcol, rcol), left, right)


def resolve_conditions(specs, left: ir.Table, right: ir.Table) -> ir.BooleanValue:
    if isinstance(specs, tuple) and len(specs) == 2:
        specs = [specs]
    specs = _util.promote_list(specs)
    return ibis.and_(*[resolve_condition(spec, left, right) for spec in specs])
