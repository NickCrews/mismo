from __future__ import annotations

import functools
from typing import overload

import ibis
from ibis.expr import types as ir

from mismo._util import optional_import


@overload
def are_aliases(name1: ir.StringValue, name2: ir.StringValue) -> ir.BooleanValue: ...


@overload
def are_aliases(name1: str, name2: str) -> bool: ...


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
) -> ir.BooleanValue: ...


@overload
def is_nickname_for(nickname: str, canonical: str) -> bool: ...


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
    return result


def _are_aliases(name1: ir.StringValue, name2: ir.StringValue) -> ir.BooleanValue:
    name1 = name1.lower().strip()
    name2 = name2.lower().strip()
    needle = ibis.struct({"name1": name1, "name2": name2})
    result = needle.isin(_aliases_column()) | (name1 == name2)
    return result


def _nicknames_pairs() -> set[tuple[str, str]]:
    with optional_import("nicknames"):
        from nicknames import NickNamer

    nn = NickNamer()
    pairs = set()
    for canonical, nicknames in nn._nickname_lookup.items():
        for nickname in nicknames:
            pairs.add((canonical, nickname))
    return pairs


def _aliases_pairs() -> set[tuple[str, str]]:
    pairs = set()
    for canonical, nickname in _nicknames_pairs():
        pairs.add((canonical, nickname))
        pairs.add((nickname, canonical))
    return pairs


@functools.cache
def _nicknames_column() -> ir.StructColumn:
    pairs = _nicknames_pairs()
    records = [{"canonical": c, "nickname": n} for c, n in pairs]
    t = ibis.memtable(
        {"pairs": records},
        schema={"pairs": "struct<canonical: string, nickname: string>"},
    )
    return t.pairs


@functools.cache
def _aliases_column() -> ir.StructColumn:
    pairs = _aliases_pairs()
    records = [{"name1": c, "name2": n} for c, n in pairs]
    t = ibis.memtable(
        {"pairs": records},
        schema={"pairs": "struct<name1: string, name2: string>"},
    )
    return t.pairs
