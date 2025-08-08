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
                    "street1": "1 1st Ave",
                    "street2": None,
                    "city": "Springfield",
                    "state": "",
                    "postal_code": "62701",
                },
            ],
            {
                "addresses": [
                    {
                        "is_pobox": False,
                        "street1": "132 MAIN ST",
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "street_trigrams": [
                            "132",
                            "32 ",
                            "2 M",
                            " MA",
                            "MAI",
                            "AIN",
                            "IN ",
                            "N S",
                            " ST",
                        ],
                        "street_name": "MAIN",
                        "street_number": "132",
                        "street_number_sorted": "123",
                    },
                    {
                        "is_pobox": False,
                        "street1": "1 1ST AVE",
                        "city": "SPRINGFIELD",
                        "state": None,
                        "postal_code": "62701",
                        "street_trigrams": [
                            "1 1",
                            " 1S",
                            "1ST",
                            "ST ",
                            "T A",
                            " AV",
                            "AVE",
                        ],
                        "street_name": "1ST",
                        "street_number": "1",
                        "street_number_sorted": "1",
                    },
                ],
                "addresses_keywords": [
                    " 1S",
                    " AV",
                    " MA",
                    " ST",
                    "1 1",
                    "132",
                    "1ST",
                    "2 M",
                    "32 ",
                    "AIN",
                    "AVE",
                    "IN ",
                    "MAI",
                    "N S",
                    "ST ",
                    "T A",
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
                        "is_pobox": False,
                        "street1": "132 MAIN ST",
                        "city": None,
                        "state": "IL IL",
                        "postal_code": "62701",
                        "street_trigrams": [
                            "132",
                            "32 ",
                            "2 M",
                            " MA",
                            "MAI",
                            "AIN",
                            "IN ",
                            "N S",
                            " ST",
                        ],
                        "street_name": "MAIN",
                        "street_number": "132",
                        "street_number_sorted": "123",
                    }
                ],
                "addresses_keywords": [
                    "132",
                    "32 ",
                    "2 M",
                    " MA",
                    "MAI",
                    "AIN",
                    "IN ",
                    "N S",
                    " ST",
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
                "addresses": [
                    {
                        "city": None,
                        "is_pobox": None,
                        "postal_code": None,
                        "state": None,
                        "street1": None,
                        "street_name": None,
                        "street_trigrams": None,
                        "street_number": None,
                        "street_number_sorted": None,
                    },
                ],
                "addresses_keywords": [],
            },
            id="all_null",
        ),
        pytest.param(
            [],
            {"addresses": [], "addresses_keywords": []},
            id="empty",
        ),
        pytest.param(None, {"addresses": None, "addresses_keywords": None}, id="null"),
    ],
)
def test_addresses_dimension(addresses, expected, table_factory):
    address_type = "array<struct<street1: string, street2: string, city: string, state: string, postal_code: string, country: string>>"  # noqa: E501
    t = table_factory({"addresses": [addresses]}, schema={"addresses": address_type})
    dim = geo.AddressesDimension("addresses")
    result = (
        dim.prepare_for_blocking(dim.prepare_for_fast_linking(t))
        .addresses_featured.execute()
        .iloc[0]
    )

    def setify(x):
        return set(x) if x is not None else None

    if result["addresses"] is not None:
        for address in result["addresses"]:
            address["street_trigrams"] = setify(address["street_trigrams"])
    if expected["addresses"] is not None:
        for address in expected["addresses"]:
            address["street_trigrams"] = setify(address["street_trigrams"])
    expected["addresses_keywords"] = setify(expected["addresses_keywords"])
    result["addresses_keywords"] = setify(result["addresses_keywords"])
    assert result == expected
