from __future__ import annotations

import ibis

from mismo import text


def test_re_extract_struct():
    s = ibis.literal("2024-03-01")
    pattern = r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)"
    result = text.re_extract_struct(s, pattern).execute()
    assert result == {"year": "2024", "month": "03", "day": "01"}


def test_re_extract_struct_case_insensitive():
    s = ibis.literal("Hello World")
    pattern = r"HELLO (?P<word>\w+)"

    result = text.re_extract_struct(s, pattern, case_insensitive=True).execute()
    assert result == {"word": "World"}

    result = text.re_extract_struct(s, pattern, case_insensitive=False).execute()
    assert result == {"word": ""}

    result = text.re_extract_struct(s, pattern).execute()
    assert result == {"word": ""}
