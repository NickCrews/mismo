from __future__ import annotations

import pytest

from mismo.lib import geo


@pytest.mark.parametrize(
    "addresses, expected",
    [
        pytest.param(
            [
                {
                    "street1": "123 Main St",
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
                        "street1": "123 MAIN ST",
                        "street2": "APT 1B",
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "country": "US",
                        "street_ngrams": [
                            "123 ",
                            "MAIN",
                            "23 M",
                            "AIN ",
                            "3 MA",
                            "IN S",
                            " MAI",
                            "N ST",
                            "APT ",
                            "PT 1",
                            "T 1B",
                            "123 MAIN ST",
                            "APT 1B",
                        ],
                        "street_no_number": "MAIN ST",
                        "street_number": "123",
                    },
                    {
                        "street1": "1 1ST",
                        "street2": None,
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "country": None,
                        "street_ngrams": [
                            "1 1S",
                            " 1ST",
                            "1 1ST",
                            None,
                        ],
                        "street_no_number": "1ST",
                        "street_number": "1",
                    },
                ],
                "addresses_keywords": [
                    "123 ",
                    "MAIN",
                    "23 M",
                    "AIN ",
                    "3 MA",
                    "IN S",
                    " MAI",
                    "N ST",
                    "APT ",
                    "PT 1",
                    "T 1B",
                    "123 MAIN ST",
                    "APT 1B",
                    "1 1S",
                    " 1ST",
                    "1 1ST",
                    None,
                ],
            },
            id="multi",
        ),
        pytest.param(
            [
                {
                    "street1": "123 Main St",
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
                        "street1": "123 MAIN ST",
                        "street2": None,
                        "city": None,
                        "state": "IL IL",
                        "postal_code": "62701",
                        "country": "US",
                        "street_ngrams": [
                            "123 ",
                            "MAIN",
                            "23 M",
                            "AIN ",
                            "3 MA",
                            "IN S",
                            " MAI",
                            "N ST",
                            "123 MAIN ST",
                            None,
                        ],
                        "street_no_number": "MAIN ST",
                        "street_number": "123",
                    }
                ],
                "addresses_keywords": [
                    "123 ",
                    "23 M",
                    "3 MA",
                    " MAI",
                    "MAIN",
                    "AIN ",
                    "IN S",
                    "N ST",
                    "123 MAIN ST",
                    None,
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
    expected["addresses_keywords"] = set(expected["addresses_keywords"])
    result["addresses_keywords"] = set(result["addresses_keywords"])
    print(result)
    assert result == expected
