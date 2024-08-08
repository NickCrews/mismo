from __future__ import annotations

import ibis
import pytest

from mismo.lib import geo


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
            {"123", "MAIN", "ST", "APT", "1", "SPRINGFIELD", "IL", "62701"},
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
            {"123", "MAIN", "ST", "IL", "62701"},
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
        (None, set()),
    ],
)
def test_addresses_dimension(address, expected, table_factory):
    address_type = "struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>"  # noqa: E501
    t = table_factory({"address": [address]}, schema={"address": address_type})
    t = t.mutate(addresses=ibis.array([t.address]))
    dim = geo.AddressesDimension("addresses")
    result = dim.prepare(t).addresses_tokens.execute()
    print(result)
    result = result.iloc[0]
    if expected is None:
        assert result is None
    else:
        assert set(result) == expected
