from __future__ import annotations

import dataclasses
import functools

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _util

_TAGGING_TYPE = dt.Array(value_type=dt.Struct({"token": "string", "label": "string"}))


@dataclasses.dataclass(frozen=True)
class TaggedAddress:
    """Tagged tokens in an address string.

    This is a convenience dataclass to work with the output of `spacy_tag_address`.
    """

    oneline: ir.StringValue
    taggings: ir.ArrayValue

    @classmethod
    def from_oneline(cls, oneline: ir.StringValue) -> TaggedAddress:
        """Create an `AddressTagging` from a single-line address string"""
        return cls(oneline, spacy_tag_address(oneline))

    @property
    def AddressNumber(self) -> ir.StringValue:
        """Merge all `AddressNumber` taggings into one string"""
        return self._get_field("AddressNumber")

    @property
    def AddressNumberPrefix(self) -> ir.StringValue:
        """Merge all `AddressNumberPrefix` taggings into one string"""
        return self._get_field("AddressNumberPrefix")

    @property
    def AddressNumberSuffix(self) -> ir.StringValue:
        """Merge all `AddressNumberSuffix` taggings into one string"""
        return self._get_field("AddressNumberSuffix")

    @property
    def BuildingName(self) -> ir.StringValue:
        """Merge all `BuildingName` taggings into one string"""
        return self._get_field("BuildingName")

    @property
    def CornerOf(self) -> ir.StringValue:
        """Merge all `CornerOf` taggings into one string"""
        return self._get_field("CornerOf")

    @property
    def CountryName(self) -> ir.StringValue:
        """Merge all `CountryName` taggings into one string"""
        return self._get_field("CountryName")

    @property
    def IntersectionSeparator(self) -> ir.StringValue:
        """Merge all `IntersectionSeparator` taggings into one string"""
        return self._get_field("IntersectionSeparator")

    @property
    def LandmarkName(self) -> ir.StringValue:
        """Merge all `LandmarkName` taggings into one string"""
        return self._get_field("LandmarkName")

    @property
    def NotAddress(self) -> ir.StringValue:
        """Merge all `NotAddress` taggings into one string"""
        return self._get_field("NotAddress")

    @property
    def OccupancyIdentifier(self) -> ir.StringValue:
        """Merge all `OccupancyIdentifier` taggings into one string"""
        return self._get_field("OccupancyIdentifier")

    @property
    def OccupancyType(self) -> ir.StringValue:
        """Merge all `OccupancyType` taggings into one string"""
        return self._get_field("OccupancyType")

    @property
    def PlaceName(self) -> ir.StringValue:
        """Merge all `PlaceName` taggings into one string"""
        return self._get_field("PlaceName")

    @property
    def Recipient(self) -> ir.StringValue:
        """Merge all `Recipient` taggings into one string"""
        return self._get_field("Recipient")

    @property
    def StateName(self) -> ir.StringValue:
        """Merge all `StateName` taggings into one string"""
        return self._get_field("StateName")

    @property
    def StreetName(self) -> ir.StringValue:
        """Merge all `StreetName` taggings into one string"""
        return self._get_field("StreetName")

    @property
    def StreetNamePostDirectional(self) -> ir.StringValue:
        """Merge all `StreetNamePostDirectional` taggings into one string"""
        return self._get_field("StreetNamePostDirectional")

    @property
    def StreetNamePostModifier(self) -> ir.StringValue:
        """Merge all `StreetNamePostModifier` taggings into one string"""
        return self._get_field("StreetNamePostModifier")

    @property
    def StreetNamePostType(self) -> ir.StringValue:
        """Merge all `StreetNamePostType` taggings into one string"""
        return self._get_field("StreetNamePostType")

    @property
    def StreetNamePreDirectional(self) -> ir.StringValue:
        """Merge all `StreetNamePreDirectional` taggings into one string"""
        return self._get_field("StreetNamePreDirectional")

    @property
    def StreetNamePreModifier(self) -> ir.StringValue:
        """Merge all `StreetNamePreModifier` taggings into one string"""
        return self._get_field("StreetNamePreModifier")

    @property
    def StreetNamePreType(self) -> ir.StringValue:
        """Merge all `StreetNamePreType` taggings into one string"""
        return self._get_field("StreetNamePreType")

    @property
    def SubaddressIdentifier(self) -> ir.StringValue:
        """Merge all `SubaddressIdentifier` taggings into one string"""
        return self._get_field("SubaddressIdentifier")

    @property
    def SubaddressType(self) -> ir.StringValue:
        """Merge all `SubaddressType` taggings into one string"""
        return self._get_field("SubaddressType")

    @property
    def USPSBoxGroupID(self) -> ir.StringValue:
        """Merge all `USPSBoxGroupID` taggings into one string"""
        return self._get_field("USPSBoxGroupID")

    @property
    def USPSBoxGroupType(self) -> ir.StringValue:
        """Merge all `USPSBoxGroupType` taggings into one string"""
        return self._get_field("USPSBoxGroupType")

    @property
    def USPSBoxID(self) -> ir.StringValue:
        """Merge all `USPSBoxID` taggings into one string"""
        return self._get_field("USPSBoxID")

    @property
    def USPSBoxType(self) -> ir.StringValue:
        """Merge all `USPSBoxType` taggings into one string"""
        return self._get_field("USPSBoxType")

    @property
    def ZipCode(self) -> ir.StringValue:
        """Merge all `ZipCode` taggings into one string"""
        return self._get_field("ZipCode")

    @property
    def ZipPlus4(self) -> ir.StringValue:
        """Merge all `ZipPlus4` taggings into one string"""
        return self._get_field("ZipPlus4")

    def _get_field(self, label: str) -> ir.StringValue:
        return (
            self.taggings.filter(lambda x: x.label == label)
            .map(lambda x: x.token)
            .join(" ")
        )


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

    >>> spacy_tag_address(
    ...     ibis.literal("456 E St Jude Stre, Oklahoma City, Oklhoma 73102-1234")
    ... ).execute()
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
