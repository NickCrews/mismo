from __future__ import annotations

import sys

import ibis
import pytest

from mismo.lib import geo


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
def test_postal_parse_address(to_shape, address, expected):
    address = ibis.literal(address, type=str)
    e = to_shape.call(geo.postal_parse_address, address)
    result = e.execute()
    assert result == expected


@pytest.mark.skipif(sys.platform == "win32", reason="postal not tested for Windows dev")
@pytest.mark.skipif(
    sys.platform == "darwin",
    # For some reason the mac install of postal doesn't have this symbol?
    # dlopen(.../_near_dupe.cpython-312-darwin.so, 0x0002): symbol not found in flat namespace '_libpostal_near_dupe_name_hashes'  # noqa: E501
    reason="mac install of postal doesn't have this function",
)
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
def test_postal_fingerprint_address(to_shape, address, expected):
    a = ibis.literal(
        address,
        type="struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>",  # noqa
    )
    e = to_shape.call(geo.postal_fingerprint_address, a)
    result = e.execute()
    assert result == expected
