from __future__ import annotations

import ibis

from mismo import _util


def test_intify_column():
    inp = ibis.memtable(
        {
            "x": ["b", "b", "a", "c", None],
            "ints": [1, 1, 0, 2, 3],
        }
    )
    inp = inp.mutate(y=inp.x)
    inted, restore = _util.intify_column(inp, "x")
    assert inted.x.type() == ibis.literal(4, type="uint64").type()
    assert (inted.x == inted.ints_expected).all().execute()
    restored = restore(inted)
    assert (restored.x == restored.original).all().execute()
