from __future__ import annotations

import ibis
import pytest

from mismo import text


@pytest.mark.parametrize(
    "inp,exp",
    [
        ("jane's   house", "jane's house"),
        ("Ross' house  ", "Ross' house"),
        ("a", "a"),
        ("", ""),
        (" ", ""),
        (None, None),
        ("bees\tall cook", "bees all cook"),
    ],
)
def test_norm_whitespace(inp, exp):
    inp = ibis.literal(inp, type="string")
    result = text.norm_whitespace(inp).execute()
    assert result == exp


def test_strip_accents_non_duckdb_raises():
    # explicitly disconnect: a GC'd sqlite3 connection raises an unraisable
    # exception on python 3.13+, which pytest pins on an arbitrary test
    sqlite_con = ibis.sqlite.connect(":memory:")
    try:
        t = sqlite_con.create_table("t", schema={"s": "string"})
        with pytest.raises(NotImplementedError):
            text.strip_accents(t.s)
    finally:
        sqlite_con.disconnect()
