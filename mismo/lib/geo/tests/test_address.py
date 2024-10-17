from __future__ import annotations

import pytest

from mismo.lib import geo


@pytest.mark.parametrize(
    "addresses, expected",
    [
        pytest.param(
            [
                {
                    "street1": "132 Main St",
                    "street2": "Apt    1-b",
                    "city": "Springfield",
                    "state": "",
                    "postal_code": "62701",
                    "country": "US",
                },
                {
                    "street1": "1 1st",
                    "street2": None,
                    "city": "Springfield",
                    "state": "",
                    "postal_code": "62701",
                },
            ],
            {
                "addresses": [
                    {
                        "street1": "132 MAIN ST",
                        "street2": "APT 1B",
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "country": "US",
                        "street_ngrams": [
                            "123",
                            "MAIN",
                        ],
                        "street_name": "MAIN",
                        "street_number": "132",
                        "street_number_sorted": "123",
                        "taggings": [
                            {
                                "label": "AddressNumber",
                                "token": "132",
                            },
                            {
                                "label": "StreetName",
                                "token": "MAIN",
                            },
                            {
                                "label": "StreetNamePostType",
                                "token": "ST",
                            },
                        ],
                    },
                    {
                        "street1": "1 1ST",
                        "street2": None,
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "country": None,
                        "street_ngrams": [
                            "1",
                            "1ST",
                        ],
                        "street_name": "1ST",
                        "street_number": "1",
                        "street_number_sorted": "1",
                        "taggings": [
                            {
                                "label": "AddressNumber",
                                "token": "1",
                            },
                            {
                                "label": "StreetName",
                                "token": "1ST",
                            },
                        ],
                    },
                ],
                "addresses_keywords": [
                    "123",
                    "MAIN",
                    "1",
                    "1ST",
                ],
            },
            id="multi",
        ),
        pytest.param(
            [
                {
                    "street1": "132 Main St",
                    "street2": None,
                    "city": None,
                    "state": "IL IL",
                    "postal_code": "62701",
                    "country": "US",
                }
            ],
            {
                "addresses": [
                    {
                        "street1": "132 MAIN ST",
                        "street2": None,
                        "city": None,
                        "state": "IL IL",
                        "postal_code": "62701",
                        "country": "US",
                        "street_ngrams": [
                            "123",
                            "MAIN",
                        ],
                        "street_name": "MAIN",
                        "street_number": "132",
                        "street_number_sorted": "123",
                        "taggings": [
                            {
                                "label": "AddressNumber",
                                "token": "132",
                            },
                            {
                                "label": "StreetName",
                                "token": "MAIN",
                            },
                            {
                                "label": "StreetNamePostType",
                                "token": "ST",
                            },
                        ],
                    }
                ],
                "addresses_keywords": [
                    "123",
                    "MAIN",
                ],
            },
            id="single-weird-state",
        ),
        pytest.param(
            [
                {
                    "street1": None,
                    "street2": None,
                    "city": None,
                    "state": None,
                    "postal_code": None,
                    "country": None,
                }
            ],
            {
                "addresses": [],
                "addresses_keywords": [],
            },
            id="all_null",
        ),
        pytest.param(
            [],
            {"addresses": [], "addresses_keywords": []},
            id="empty",
        ),
        pytest.param(None, {"addresses": [], "addresses_keywords": []}, id="null"),
    ],
)
def test_addresses_dimension(addresses, expected, table_factory):
    address_type = "array<struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>>"  # noqa: E501
    t = table_factory({"addresses": [addresses]}, schema={"addresses": address_type})
    dim = geo.AddressesDimension("addresses")
    result = dim.prepare(t).addresses_featured.execute().iloc[0]
    for address in result["addresses"]:
        address["street_ngrams"] = set(address["street_ngrams"])
    for address in expected["addresses"]:
        address["street_ngrams"] = set(address["street_ngrams"])
    expected["addresses_keywords"] = set(expected["addresses_keywords"])
    result["addresses_keywords"] = set(result["addresses_keywords"])
    assert result == expected
