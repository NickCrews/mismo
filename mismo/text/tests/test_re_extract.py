from __future__ import annotations

import ibis

from mismo import text


def test_re_extract_struct():
    s = ibis.literal("2024-03-01")
    pattern = r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)"
    result = text.re_extract_struct(s, pattern).execute()
    assert result == {"year": "2024", "month": "03", "day": "01"}
