from __future__ import annotations

import functools
from typing import overload

import ibis
from ibis.expr import types as ir

from mismo._util import optional_import


@overload
def are_aliases(name1: ir.StringValue, name2: ir.StringValue) -> ir.BooleanValue:
    ...


@overload
def are_aliases(name1: str, name2: str) -> bool:
    ...


def are_aliases(name1, name2):
    """Determine if two names are nickname interchangeable.

    This is case-insensitive, and whitespace is stripped from both ends.
    The same name returns True.
    """
    is_string = False
    if isinstance(name1, str) and isinstance(name2, str):
        is_string = True
    if isinstance(name1, str):
        name1 = ibis.literal(name1)
    if isinstance(name2, str):
        name2 = ibis.literal(name2)
    result = _are_aliases(name1, name2)
    if is_string:
        return bool(result.execute())
    return result


@overload
def is_nickname_for(
    nickname: ir.StringValue, canonical: ir.StringValue
) -> ir.BooleanValue:
    ...


@overload
def is_nickname_for(nickname: str, canonical: str) -> bool:
    ...


def is_nickname_for(nickname, canonical):
    """Determine if a name is a nickname for another name.

    This is case-insensitive, and whitespace is stripped from both ends.
    The same name returns True.
    """
    is_string = False
    if isinstance(nickname, str) and isinstance(canonical, str):
        is_string = True
    if not isinstance(nickname, ir.StringValue):
        nickname = ibis.literal(nickname)
    if not isinstance(canonical, ir.StringValue):
        canonical = ibis.literal(canonical)
    result = _is_nickname_for(nickname, canonical)
    if is_string:
        return bool(result.execute())
    return result


def _is_nickname_for(
    nickname: ir.StringValue, canonical: ir.StringValue
) -> ir.BooleanValue:
    nickname = nickname.lower().strip()
    canonical = canonical.lower().strip()
    needle = ibis.struct({"canonical": canonical, "nickname": nickname})
    result = needle.isin(_nicknames_column()) | (canonical == nickname)
    # workaround for https://github.com/ibis-project/ibis/issues/8361
    if isinstance(needle, ir.Scalar):
        return result.as_scalar()
    return result


def _are_aliases(name1: ir.StringValue, name2: ir.StringValue) -> ir.BooleanValue:
    name1 = name1.lower().strip()
    name2 = name2.lower().strip()
    needle = ibis.struct({"name1": name1, "name2": name2})
    result = needle.isin(_aliases_column()) | (name1 == name2)
    # workaround for https://github.com/ibis-project/ibis/issues/8361
    if isinstance(needle, ir.Scalar):
        return result.as_scalar()
    return result


@functools.cache
def _nicknames_column() -> ir.StructColumn:
    with optional_import("nicknames"):
        from nicknames import NickNamer

    nn = NickNamer()
    pairs = []
    for canonical, nicknames in nn._nickname_lookup.items():
        for nickname in nicknames:
            pairs.append((canonical, nickname))
    t = ibis.memtable(pairs, columns=["canonical", "nickname"])
    s = ibis.struct({"canonical": t.canonical, "nickname": t.nickname})
    s = s.name("nicknames").as_table().cache().nicknames
    return s


@functools.cache
def _aliases_column() -> ir.StructColumn:
    nn = _nicknames_column()
    a1 = ibis.struct({"name1": nn.canonical, "name2": nn.nickname})
    a2 = ibis.struct({"name1": nn.nickname, "name2": nn.canonical})
    a1 = a1.name("aliases").as_table()
    a2 = a2.name("aliases").as_table()
    t = ibis.union(a1, a2).distinct()
    t = t.cache()
    return t.aliases
