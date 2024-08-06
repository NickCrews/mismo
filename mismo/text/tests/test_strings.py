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
