from __future__ import annotations

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
