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
