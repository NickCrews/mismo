from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

import ibis
from ibis.expr import types as ir


def mutate(struct: ir.StructValue, **kwargs: ir.Value) -> ir.StructValue:
    """Mutate a struct by adding or replacing columns.

    Analogous to ibis.Table.mutate(**kwargs).
    """
    default = {field: struct[field] for field in struct.fields}
    return ir.struct({**default, **kwargs})


def drop(struct: ir.StructValue, *fields: str) -> ir.StructValue:
    """Mutate a struct by dropping columns.

    Analogous to ibis.Table.drop(fields)."""
    new_fields = {
        field: struct[field] for field in struct.fields if field not in fields
    }
    return ir.struct(new_fields)


def select(struct: ir.Column, *fields: str) -> ir.Column:
    """Select columns from a struct.

    Analogous to ibis.Table.select(fields)."""
    return ir.struct({field: struct[field] for field in fields})


def rename(struct: ir.StructValue, **renamings: str) -> ir.StructValue:
    """Rename the fields in a struct, analogous to Table.rename()"""
    fields = {field: struct[field] for field in struct.fields}
    for new, old in renamings.items():
        fields[new] = fields.pop(old)
    return ir.struct(fields)


def unpack(struct: ir.StructValue) -> tuple[ir.Value, ...]:
    """Unpack the values of a struct into a tuple.

    Replacement for the deprecated struct.destructure() method,
    and analogous to the Table.unpack("my_struct_col") method.
    """
    return (struct[field_name].name(field_name) for field_name in struct.type().names)


def struct_equal(
    left: ir.StructValue, right: ir.StructValue, *, fields: Iterable[str] | None = None
) -> ir.BooleanValue:
    """
    The specified fields match exactly. If fields is None, all fields are compared.
    """
    if fields is None:
        return left == right
    return ibis.and_(*(left[f] == right[f] for f in fields))


def struct_isnull(
    struct: ir.StructValue, *, how: Literal["any", "all"], fields: Iterable[str] | None
) -> ir.BooleanValue:
    """Are any/all of the specified fields null (or the struct itself is null)?

    If fields is None, all fields are compared."""
    if fields is None:
        fields = struct.type().names
    vals = [struct[f].isnull() for f in fields]
    if how == "any":
        return struct.isnull() | ibis.or_(*vals)
    elif how == "all":
        return struct.isnull() | ibis.and_(*vals)
    else:
        raise ValueError(f"how must be 'any' or 'all'. Got {how}")
