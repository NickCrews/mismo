from __future__ import annotations

import functools
from typing import overload

import ibis
from ibis.expr.types import BooleanValue, Scalar, StringValue, Table

from mismo._util import optional_import


@overload
def are_aliases(name1: StringValue, name2: StringValue) -> BooleanValue:
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
def is_nickname_for(nickname: StringValue, canonical: StringValue) -> BooleanValue:
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
    if not isinstance(nickname, StringValue):
        nickname = ibis.literal(nickname)
    if not isinstance(canonical, StringValue):
        canonical = ibis.literal(canonical)
    result = _is_nickname_for(nickname, canonical)
    if is_string:
        return bool(result.execute())
    return result


def _is_nickname_for(nickname: StringValue, canonical: StringValue) -> BooleanValue:
    nickname = nickname.lower().strip()
    canonical = canonical.lower().strip()
    NICKNAMES = _nicknames_table()
    haystack = ibis.struct(
        {"canonical": NICKNAMES.canonical, "nickname": NICKNAMES.nickname}
    )
    needle = ibis.struct({"canonical": canonical, "nickname": nickname})
    result = needle.isin(haystack) | (canonical == nickname)
    # workaround for https://github.com/ibis-project/ibis/issues/8361
    if isinstance(needle, Scalar):
        return result.as_scalar()
    return result


def _are_aliases(name1: StringValue, name2: StringValue) -> BooleanValue:
    return is_nickname_for(name1, name2) | is_nickname_for(name2, name1)


@functools.cache
def _nicknames_table() -> Table:
    with optional_import():
        from nicknames import NickNamer

    nn = NickNamer()
    pairs = []
    for canonical, nicknames in nn._nickname_lookup.items():
        for nickname in nicknames:
            pairs.append((canonical, nickname))
    return ibis.memtable(pairs, columns=["canonical", "nickname"])
