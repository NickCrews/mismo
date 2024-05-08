from __future__ import annotations

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


@pytest.mark.parametrize(
    "address, expected",
    [
        (
            "123 Main St, Springfield, IL, 62701, US",
            {
                "street1": "123 main st",
                "street2": "",
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
                "street2": "",
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
        ),
    ],
)
def test_parse_address(address, expected):
    result = _address.parse_address(address).execute()
    assert result == expected


@pytest.mark.parametrize(
    "address, expected",
    [
        (
            "123 Main St, Springfield, IL, 62701, US",
            [
                "act|main saint|123|springfield",
                "act|main street|123|springfield",
                "act|main|123|springfield",
                "apc|main saint|123|62701",
                "apc|main street|123|62701",
                "apc|main|123|62701",
            ],
        )
    ],
)
def test_hash_address(address, expected):
    result = _address.hash_address(address).execute()
    assert result == expected


@pytest.mark.parametrize(
    "address1, address2, expected",
    [
        (
            {
                "street1": "one hundred twenty three",
                "street2": "main st",
                "city": "springfield",
                "state": "il",
                "postal_code": "62701",
                "country": "us",
            },
            {
                "street1": "123",
                "street2": "main road",
                "city": "springfield",
                "state": "wi",
                "postal_code": "62701",
                "country": "us",
            },
            {
                "street1": "EXACT_DUPLICATE",
                "street2": "NEEDS_REVIEW",
                "city": "EXACT_DUPLICATE",
                "state": "NON_DUPLICATE",
                "postal_code": "EXACT_DUPLICATE",
                "country": "EXACT_DUPLICATE",
            },
        )
    ],
)
def test_compare_addresses(address1, address2, expected):
    a = ibis.literal(
        address1,
        type="struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>",  # noqa
    )
    b = ibis.literal(
        address2,
        type="struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>",  # noqa
    )
    result = _address.compare_addresses(a, b).execute()

    assert result == expected
