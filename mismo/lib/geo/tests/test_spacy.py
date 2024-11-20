from __future__ import annotations

import ibis
import pytest

from mismo.lib import geo


@pytest.mark.parametrize(
    "address, expected",
    [
        pytest.param(
            "123 Main St, Springfield, IL, 62701, US",
            [
                {"token": "123", "label": "AddressNumber"},
                {"token": "Main", "label": "StreetName"},
                {"token": "St", "label": "StreetNamePostType"},
                {"token": "Springfield", "label": "PlaceName"},
                {"token": "IL", "label": "StateName"},
                {"token": "62701", "label": "ZipCode"},
                {"token": "US", "label": "CountryName"},
            ],
            id="123main",
        ),
        pytest.param(
            "456 E St Jude Stre, Oklahoma City, Oklhoma 73102-1234",  # state has typo
            [
                {"token": "456", "label": "AddressNumber"},
                {"token": "E", "label": "StreetNamePreDirectional"},
                {"token": "St Jude", "label": "StreetName"},
                {"token": "Stre", "label": "StreetNamePostType"},
                {"token": "Oklahoma City", "label": "PlaceName"},
                {"token": "Oklhoma", "label": "StateName"},
                {"token": "73102-1234", "label": "ZipCode"},
            ],
            id="oklahoma",
        ),
        pytest.param(
            "PO Box 4-b, Adak, alaksa 99546",  # alaska is misspelled
            [
                {"token": "PO Box", "label": "USPSBoxType"},
                {"token": "4-b", "label": "USPSBoxID"},
                {"token": "Adak", "label": "PlaceName"},
                {"token": "alaksa", "label": "StateName"},
                {"token": "99546", "label": "ZipCode"},
            ],
            id="po_box",
        ),
        ("", []),
        (None, None),
    ],
)
def test_spacy_parse_address(to_shape, address, expected):
    address = ibis.literal(address, type=str)
    e = to_shape.call(geo.spacy_tag_address, address)
    result = e.execute()
    assert result == expected
