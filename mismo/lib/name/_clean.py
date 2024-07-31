from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util


def normalize_name_field(field: ir.StringValue) -> ir.StringValue:
    """Convert to uppercase, normalize whitespace, and remove non-alphanumeric.

    Parameters
    ----------
    name :
        The name to normalize.

    Returns
    -------
    name_normed :
        The normalized name.
    """
    field = field.upper()
    field = field.re_replace(r"[^A-Za-z0-9]+", " ")
    field = field.re_replace(r"\s+", " ")
    field = field.strip()
    return field


def normalize_name(name: ir.StructValue) -> ir.StructValue:
    """Convert to uppercase, normalize whitespace, and remove non-alphanumeric.

    Parameters
    ----------
    name :
        The name to normalize.

    Returns
    -------
    name_normed :
        The normalized name.
    """
    return ibis.struct(
        {
            "prefix": normalize_name_field(name.prefix),
            "given": normalize_name_field(name.given),
            "middle": normalize_name_field(name.middle),
            "surname": normalize_name_field(name.surname),
            "suffix": normalize_name_field(name.suffix),
            "nickname": normalize_name_field(name.nickname),
        }
    )


def name_tokens(name: ir.StructValue, *, unique: bool = True) -> ir.ArrayValue:
    """Get all the tokens from a name."""
    return _util.struct_tokens(name, unique=unique)
