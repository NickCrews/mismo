from __future__ import annotations

import ibis

from mismo import _funcs, _util


def get_keys_2_tables(
    t1: ibis.Table, t2: ibis.Table, keys
) -> tuple[tuple[ibis.Value], tuple[ibis.Value]]:
    keys = _util.promote_list(keys)
    lkeys = []
    rkeys = []
    for key in keys:
        keyl, keyr = _get_keys_2_tables(t1, t2, key)
        lkeys.append(keyl)
        rkeys.append(keyr)
    return tuple(lkeys), tuple(rkeys)


ERROR_TEMPLATE = (
    "Key {key} is not a valid key. Must be a ibis.Value, str, Deferred,"
    "2-tuple of the above, a function (Table)->Value or (Table, Table)->(Value, Value)."  # noqa: E501
)


def _get_keys_2_tables(
    t1: ibis.Table, t2: ibis.Table, key
) -> tuple[ibis.Value, ibis.Value]:
    if isinstance(key, ibis.Value):
        return key, key
    if isinstance(key, ibis.Deferred):
        return _bind_one(t1, key), _bind_one(t2, key)
    if isinstance(key, str):
        return _bind_one(t1, key), _bind_one(t2, key)
    if _funcs.is_unary(key):
        return _bind_one(t1, key), _bind_one(t2, key)
    if _funcs.is_binary(key):
        resolved = key(t1, t2)
        return _get_keys_2_tables(t1, t2, resolved)
    parts = _util.promote_list(key)
    if len(parts) != 2:
        raise ValueError(ERROR_TEMPLATE.format(key=key))
    keyl, keyr = parts
    return _bind_one(t1, keyl), _bind_one(t2, keyr)


def _bind_one(t: ibis.Table, key) -> ibis.Value:
    result = t.bind(key)
    if len(result) == 1:
        return result[0]
    raise ValueError(ERROR_TEMPLATE.format(key=key))
