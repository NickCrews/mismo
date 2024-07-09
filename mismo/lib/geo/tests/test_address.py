from __future__ import annotations

import sys

import ibis
import pytest

from mismo.lib.geo import _address


@pytest.mark.parametrize(
    "address, expected",
    [
        (
            {
                "street1": "123 Main St",
                "street2": "Apt    1",
                "city": "Springfield",
                "state": "IL",
                "postal_code": "62701",
                "country": "US",
            },
            {"123", "Main", "St", "Apt", "1", "Springfield", "IL", "62701", "US"},
        ),
        (
            {
                "street1": "123 Main St",
                "street2": None,
                "city": None,
                "state": "IL IL",
                "postal_code": "62701",
                "country": "US",
            },
            {"123", "Main", "St", "IL", "62701", "US"},
        ),
        (
            {
                "street1": None,
                "street2": None,
                "city": None,
                "state": None,
                "postal_code": None,
                "country": None,
            },
            set(),
        ),
        (None, None),
    ],
)
def test_address_tokens(address, expected):
    a = ibis.literal(
        address,
        type="struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>",  # noqa
    )
    result = _address.address_tokens(a).execute()
    if expected is None:
        assert result is None
    else:
        assert set(result) == expected


@pytest.mark.skipif(sys.platform == "win32", reason="postal not tested for Windows dev")
@pytest.mark.parametrize(
    "address, expected",
    [
        (
            "123 Main St, Springfield, IL, 62701, US",
            {
                "street1": "123 main st",
                "street2": None,
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
        (
            "Apt. A, 123 Main St, Springfield, IL, 62701, US",
            {
                "street1": "123 main st",
                "street2": "apt. a",
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
        (
            "Main St, Springfield, IL, 62701, US",
            {
                "street1": "main st",
                "street2": None,
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
        (
            "123 Main St, Springfield, IL, 62701, US  ",
            {
                "street1": "123 main st",
                "street2": None,
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
        (
            "123    Main St,    Springfield,    IL, 62701,  US",
            {
                "street1": "123 main st",
                "street2": None,
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
        (
            "",
            {
                "street1": None,
                "street2": None,
                "city": None,
                "state": None,
                "postal_code": None,
                "country": None,
            },
        ),
        (None, None),
        (
            "foo bar baz",
            {
                "street1": None,
                "street2": None,
                "city": None,
                "state": None,
                "postal_code": None,
                "country": None,
            },
        ),
        pytest.param(
            "〒150-2345 東京都渋谷区本町2丁目4-7サニーマンション203",
            {
                "street1": "2丁目4-7 本町",
                "street2": None,
                "city": "渋谷区",
                "state": "東京都",
                "postal_code": "〒150-2345",
                "country": None,
            },
            marks=pytest.mark.xfail(reason="postal incorrectly parses street and city"),
        ),
    ],
)
def test_parse_address(address, expected):
    result = _address.postal_parse_address(address).execute()
    assert result == expected


@pytest.mark.skipif(sys.platform == "win32", reason="postal not tested for Windows dev")
@pytest.mark.parametrize(
    "address, expected",
    [
        (
            {
                "street1": "123 Main Street",
                "street2": "",
                "city": "Springfield",
                "state": "IL",
                "postal_code": "62701",
                "country": "us",
            },
            [
                "act|main street|123|springfield",
                "act|main|123|springfield",
                "apc|main street|123|62701",
                "apc|main|123|62701",
            ],
        ),
        (None, None),
        (
            {
                "street1": None,
                "street2": None,
                "city": None,
                "state": None,
                "postal_code": None,
                "country": None,
            },
            [],
        ),
        (
            {
                "street1": "4-7",
                "street2": None,
                "city": "京 区",
                "state": "東",
                "postal_code": "〒150-2345",
                "country": None,
            },
            [
                "hct|4-7|10000000000000000 区",
                "hct|4-7|jing qu",
                "hpc|4-7|150-2345",
                "hpc|4-7|〒150-2345",
            ],
        ),
    ],
)
def test_hash_address(address, expected):
    a = ibis.literal(
        address,
        type="struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>",  # noqa
    )
    result = _address.postal_fingerprint_address(a).execute()
    assert result == expected
