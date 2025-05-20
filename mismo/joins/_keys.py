from __future__ import annotations

import ibis

from mismo import _resolve, _util


def get_keys_2_tables(
    keys, left: ibis.Table, right: ibis.Table
) -> tuple[tuple[ibis.Value], tuple[ibis.Value]]:
    keys = _util.promote_list(keys)
    lkeys = []
    rkeys = []
    for key in keys:
        keyl, keyr = _resolve.resolve_column_pair(key, left, right)
        lkeys.append(keyl)
        rkeys.append(keyr)
    return tuple(lkeys), tuple(rkeys)
