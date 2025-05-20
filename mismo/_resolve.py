from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _funcs, _util


def _resolve(t: ir.Table, spec) -> ir.Value:
    values = t.bind(spec)
    if len(values) != 1:
        raise ValueError(f"Expected 1 column, got {len(values)} from {spec}")
    return values[0]


def resolve_column_pair(
    spec, left: ir.Table, right: ir.Table
) -> tuple[ir.Column, ir.Column]:
    if isinstance(spec, ibis.Value):
        return spec, spec
    if isinstance(spec, ibis.Deferred):
        return _resolve(left, spec), _resolve(right, spec)
    if isinstance(spec, str):
        return _resolve(left, spec), _resolve(right, spec)
    if _funcs.is_unary(spec):
        return _resolve(left, spec), _resolve(right, spec)
    if _funcs.is_binary(spec):
        resolved = spec(left, right)
        return resolve_column_pair(resolved, left, right)
    parts = _util.promote_list(spec)
    if len(parts) != 2:
        raise ValueError(
            (
                "Column spec, when an iterable, must be length 2.",
                f" got {len(parts)} from {spec}",
            )
        )
    keyl, keyr = parts
    return _resolve(left, keyl), _resolve(right, keyr)
