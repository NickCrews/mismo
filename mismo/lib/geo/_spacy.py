from __future__ import annotations

import functools

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _util

# _RAW_TAGS = [
#     "AddressNumber",
#     "AddressNumberPrefix",
#     "AddressNumberSuffix",
#     "BuildingName",
#     "CornerOf",
#     "CountryName",
#     "IntersectionSeparator",
#     "LandmarkName",
#     "NotAddress",
#     "OccupancyIdentifier",
#     "OccupancyType",
#     "PlaceName",
#     "Recipient",
#     "StateName",
#     "StreetName",
#     "StreetNamePostDirectional",
#     "StreetNamePostModifier",
#     "StreetNamePostType",
#     "StreetNamePreDirectional",
#     "StreetNamePreModifier",
#     "StreetNamePreType",
#     "SubaddressIdentifier",
#     "SubaddressType",
#     "USPSBoxGroupID",
#     "USPSBoxGroupType",
#     "USPSBoxID",
#     "USPSBoxType",
#     "ZipCode",
#     "ZipPlus4",
# ]

_TAGGING_TYPE = dt.Array(value_type=dt.Struct({"token": "string", "label": "string"}))


def spacy_tag_address(address_string: ir.StringValue) -> ir.ArrayValue:
    """
    Tag each token in a US address string with its type, eg StreetName, StreetPreDirectional

    .. note::
    To use this function, you need the optional `spacy-address` library installed
    from https://github.com/NickCrews/spacy-address

    This a trained Named Entity Recognition (NER) model in spaCy
    to tag tokens in an address string with the following labels:

    - AddressNumber
    - AddressNumberPrefix
    - AddressNumberSuffix
    - BuildingName
    - CornerOf
    - CountryName
    - IntersectionSeparator
    - LandmarkName
    - NotAddress
    - OccupancyIdentifier
    - OccupancyType
    - PlaceName
    - Recipient
    - StateName
    - StreetName
    - StreetNamePostDirectional
    - StreetNamePostModifier
    - StreetNamePostType
    - StreetNamePreDirectional
    - StreetNamePreModifier
    - StreetNamePreType
    - SubaddressIdentifier
    - SubaddressType
    - USPSBoxGroupID
    - USPSBoxGroupType
    - USPSBoxID
    - USPSBoxType
    - ZipCode
    - ZipPlus4

    Parameters
    ----------
    address_string
        The address as a single string

    Returns
    -------
    taggings
        An `array<struct<token: string, label: string>>` with the tagged tokens

    Examples
    --------
    >>> from mismo.lib.geo import spacy_tag_address
    >>> import ibis

    Note that
    - "St" isn't confused as an abbreviation for Street,
    - "Stre" is correctly tagged as typo for "Street"
    - "Oklahoma" in "Oklahoma City" is correctly tagged as a PlaceName
    - "Oklhoma" is correctly tagged as a typo for "Oklahoma"

    >>> spacy_tag_address(ibis.literal("456 E St Jude Stre, Oklahoma City, Oklhoma 73102-1234")).execute()
    [{'token': '456', 'label': 'AddressNumber'},
    {'token': 'E', 'label': 'StreetNamePreDirectional'},
    {'token': 'St Jude', 'label': 'StreetName'},
    {'token': 'Stre', 'label': 'StreetNamePostType'},
    {'token': 'Oklahoma City', 'label': 'PlaceName'},
    {'token': 'Oklhoma', 'label': 'StateName'},
    {'token': '73102-1234', 'label': 'ZipCode'}]
    """  # noqa: E501
    with _util.optional_import("spacy"):
        import spacy  # noqa: F401
    with _util.optional_import("en_us_address_ner_sm"):
        import en_us_address_ner_sm

    nlp = en_us_address_ner_sm.load()

    @ibis.udf.scalar.python(signature=((str,), _TAGGING_TYPE))
    @functools.cache
    def udf(address_string: str | None) -> dict[str, str] | None:
        # remove once https://github.com/ibis-project/ibis/pull/9625 is fixed
        if address_string is None:
            return None
        doc = nlp(address_string)
        return [{"token": token.text, "label": token.label_} for token in doc.ents]

    return udf(address_string)
