from __future__ import annotations

import ibis
import pytest

from mismo.lib import geo


@pytest.mark.parametrize(
    "inp,expected",
    [
        ("GENERAL DELIVERY", None),
        ("BARRACKS ST UNIT 2", None),
        (
            "4602 CR 673",
            {
                "AddressNumber": "4602",
                "StreetNamePreDirectional": "",
                "StreetName": "CR 673",
                "StreetNamePostType": "",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "6473 FM 1798",
            {
                "AddressNumber": "6473",
                "StreetNamePreDirectional": "",
                "StreetName": "FM 1798",
                "StreetNamePostType": "",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "1 1ST",
            {
                "AddressNumber": "1",
                "StreetName": "1ST",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "123 MAIN",
            {
                "AddressNumber": "123",
                "StreetName": "MAIN",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "123 S MAIN WEST",
            {
                "AddressNumber": "123",
                "StreetName": "MAIN",
                "StreetNamePostDirectional": "WEST",
                "StreetNamePostType": "",
                "StreetNamePreDirectional": "S",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "1 1ST ST",
            {
                "AddressNumber": "1",
                "StreetName": "1ST",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "ST",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "7100 RIDGE MANOR LN",
            {
                "AddressNumber": "7100",
                "StreetNamePreDirectional": "",
                "StreetName": "RIDGE MANOR",
                "StreetNamePostType": "LN",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # Mixed case is also detected
        (
            "7100 Ridge Manor ln",
            {
                "AddressNumber": "7100",
                "StreetNamePreDirectional": "",
                "StreetName": "Ridge Manor",
                "StreetNamePostType": "ln",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "1101B ODIN ST",
            {
                "AddressNumber": "1101B",
                "StreetNamePreDirectional": "",
                "StreetName": "ODIN",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "1385 JUNEAU AVE",
            {
                "AddressNumber": "1385",
                "StreetNamePreDirectional": "",
                "StreetName": "JUNEAU",
                "StreetNamePostType": "AVE",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "PO BOX 651",
            {
                "AddressNumber": "",
                "StreetNamePreDirectional": "",
                "StreetName": "",
                "StreetNamePostType": "",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "651",
            },
        ),
        (
            "p.o. Box 651",
            {
                "AddressNumber": "",
                "StreetNamePreDirectional": "",
                "StreetName": "",
                "StreetNamePostType": "",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "651",
            },
        ),
        (
            "604 FIFTH ST",
            {
                "AddressNumber": "604",
                "StreetNamePreDirectional": "",
                "StreetName": "FIFTH",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "604 E  FIFTH ST",
            {
                "AddressNumber": "604",
                "StreetNamePreDirectional": "E",
                "StreetName": "FIFTH",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "604 E  5TH ST",
            {
                "AddressNumber": "604",
                "StreetNamePreDirectional": "E",
                "StreetName": "5TH",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "44755 STERLING HWY",
            {
                "AddressNumber": "44755",
                "StreetNamePreDirectional": "",
                "StreetName": "STERLING",
                "StreetNamePostType": "HWY",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "100 MC CARREY ST",
            {
                "AddressNumber": "100",
                "StreetNamePreDirectional": "",
                "StreetName": "MC CARREY",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # periods after abbreviations are ignored
        (
            "100 MC CARREY ST.",
            {
                "AddressNumber": "100",
                "StreetNamePreDirectional": "",
                "StreetName": "MC CARREY",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "100 MC CARREY ST. W",
            {
                "AddressNumber": "100",
                "StreetNamePreDirectional": "",
                "StreetName": "MC CARREY",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "W",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "100 MC CARREY ST. W.",
            {
                "AddressNumber": "100",
                "StreetNamePreDirectional": "",
                "StreetName": "MC CARREY",
                "StreetNamePostType": "ST",
                "StreetNamePostDirectional": "W",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # But a perdiod in the middle of a street name is not ignored
        (
            "100 St. Johann Loop",
            {
                "AddressNumber": "100",
                "StreetNamePreDirectional": "",
                "StreetName": "St. Johann",
                "StreetNamePostType": "Loop",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # I kid you not this is a legal street type
        (
            "1015 OTTER RUN",
            {
                "AddressNumber": "1015",
                "StreetNamePreDirectional": "",
                "StreetName": "OTTER",
                "StreetNamePostType": "RUN",
                "StreetNamePostDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # 3 token long street name
        (
            "9950 STEPHEN RICHARDS MEMORIAL DR",
            {
                "AddressNumber": "9950",
                "StreetName": "STEPHEN RICHARDS MEMORIAL",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "DR",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # 4 token long street name
        (
            "2370 S RUE DE LA PAIX LOOP",
            {
                "AddressNumber": "2370",
                "StreetName": "RUE DE LA PAIX",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "LOOP",
                "StreetNamePreDirectional": "S",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # zero instead of O
        (
            "P 0 BOX 33",
            {
                "AddressNumber": "",
                "StreetName": "",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "33",
            },
        ),
        # apostrophe in street name
        (
            "4421 O'MALLEY RD",
            {
                "AddressNumber": "4421",
                "StreetName": "O'MALLEY",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "RD",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # hyphen in street name
        (
            "46465 HOLT-LAMPLIGHT RD",
            {
                "AddressNumber": "46465",
                "StreetName": "HOLT-LAMPLIGHT",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "RD",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        (
            "HC 1 BOX 106",
            {
                "AddressNumber": "",
                "StreetName": "",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "HC",
                "USPSBoxGroupID": "1",
                "USPSBoxID": "106",
            },
        ),
        (
            "MILE 15 ALYESKA HWY",
            {
                "AddressNumber": "15",
                "StreetName": "ALYESKA",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "HWY",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # with decimal 1
        (
            "MILE 1.8 DYEA RD",
            {
                "AddressNumber": "1.8",
                "StreetName": "DYEA",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "RD",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
        # with decimal 2
        (
            "MILE 15.3 ALYESKA HWY",
            {
                "AddressNumber": "15.3",
                "StreetName": "ALYESKA",
                "StreetNamePostDirectional": "",
                "StreetNamePostType": "HWY",
                "StreetNamePreDirectional": "",
                "USPSBoxGroupType": "",
                "USPSBoxGroupID": "",
                "USPSBoxID": "",
            },
        ),
    ],
)
def test_parse_street1_re(inp, expected):
    result = geo.parse_street1_re(ibis.literal(inp)).execute()
    assert expected == result
