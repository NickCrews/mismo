from __future__ import annotations

import ibis

from mismo import _util


def test_intify_column():
    inp = ibis.memtable(
        {
            "vals": ["b", "b", "a", "c", None],
            "ints_expected": [1, 1, 0, 2, 3],
        }
    )
    inp = inp.mutate(original=inp.vals)
    inted, restore = _util.intify_column(inp, "vals")
    assert inted.vals.type() == ibis.literal(4, type="uint64").type()
    assert (inted.vals == inted.ints_expected).all().execute()
    restored = restore(inted)
    assert (restored.vals == restored.original).all().execute()
