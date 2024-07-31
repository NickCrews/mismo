from __future__ import annotations

import ibis
import pytest

from mismo.lib import phone


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("9071238765", "9071238765"),
        ("19071238765", "9071238765"),
        ("1 907 123 8765", "9071238765"),
        ("1 907   123 8765", "9071238765"),
        ("(907) 346-2233", "9073462233"),
        ("1 (907) 346-2233", "9073462233"),
        ("1-907-123-8765", "9071238765"),
        ("1.907.123.8765", "9071238765"),
        ("907 .123.8765 ", "9071238765"),
        ("907-123-8765", "9071238765"),
        ("+19071238765", "9071238765"),
        ("+19071238765", "9071238765"),
        (" cats 3281947382   meow", "3281947382"),
        ("328194738215345345345", "3281947382"),  # uh oh this isn't great
        ("1907", None),
        ("012345678", None),  # too short
        ("9070000000", None),  # bogus
        ("9071234567", None),  # bogus
        ("9079999999", None),  # bogus
        ("0000000", None),  # bogus
        ("1234567", None),  # bogus
        ("9999999", None),  # bogus
        ("junk", None),
        (None, None),
    ],
)
@pytest.mark.parametrize("area_code", [None, "907"])
def test_clean_phone_number(inp, expected, area_code):
    inp = ibis.literal(inp, str)
    expected = ibis.literal(expected)
    result = phone.clean_phone_number(inp, default_area_code=area_code)
    assert expected.execute() == result.execute()


@pytest.mark.parametrize(
    "inp,area_code,expected",
    [
        ("9071238765", None, "9071238765"),
        ("9071238765", "907", "9071238765"),
        ("9071238765", "999", "9071238765"),
        ("1238765", None, None),
        ("1238765", "907", "9071238765"),
        (" cats 3281947382   meow", None, "3281947382"),
        (" cats 3281947382   meow", "328", "3281947382"),
        ("1 (907) 346-2233", None, "9073462233"),
        ("1 (907) 346-2233", "907", "9073462233"),
    ],
)
def test_clean_phone_number_area_code(inp, expected, area_code):
    inp = ibis.literal(inp, str)
    expected = ibis.literal(expected)
    result = phone.clean_phone_number(inp, default_area_code=area_code)
    assert expected.execute() == result.execute()


@pytest.mark.parametrize(
    "a, b, level_str",
    [
        pytest.param("19071234567", "19071234567", "EXACT", id="exact"),
        pytest.param("19071234567", "19071234576", "NEAR", id="near"),
        pytest.param("19071234567", "19071234599", "ELSE", id="two_different"),
        pytest.param("  19071234567", "19071234567", "ELSE", id="whitespace"),
    ],
)
def test_match_level(a, b, level_str):
    ml = phone.match_level(ibis.literal(a), ibis.literal(b))
    assert ml.as_string().execute() == level_str
    assert ml.as_integer().execute() == phone.PhoneMatchLevel[level_str]
