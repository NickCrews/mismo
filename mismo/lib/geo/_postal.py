from __future__ import annotations

from collections import defaultdict
import re

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _util

_ADDRESS_SCHEMA = dt.Struct(
    {
        "street1": "string",
        "street2": "string",
        "city": "string",
        "state": "string",
        "postal_code": "string",
        "country": "string",
    }
)

_DIGITS_REGEX = re.compile(r"[0-9]+")


def postal_parse_address(address_string: ir.StringValue) -> ir.StructValue:
    """Parse individual fields from an address string.

    .. note:: To use this function, you need the optional `postal` library installed.

    This uses the optional `postal` library to extract individual fields
    from the string using the following mapping:

    - house_number + road -> street1
    - unit -> street2
    - city -> city
    - state -> state
    - postcode -> postal_code
    - country -> country

    Any additional fields parsed by postal will not be included.

    Parameters
    ----------
    address_string :
        The address as a single string

    Returns
    -------
    address :
        The parsed address as a Struct
    """
    with _util.optional_import("postal"):
        from postal.parser import parse_address as _parse_address

    @ibis.udf.scalar.python(signature=((str,), _ADDRESS_SCHEMA))
    def udf(address_string: str | None) -> dict[str, str] | None:
        # remove once https://github.com/ibis-project/ibis/pull/9625 is fixed
        if address_string is None:
            return None
        parsed_fields = _parse_address(address_string)
        label_to_values = defaultdict(list)
        for value, label in parsed_fields:
            label_to_values[label].append(value)
        renamed = {
            "street1": label_to_values["house_number"] + label_to_values["road"],
            "street2": label_to_values["unit"],
            "city": label_to_values["city"],
            "state": label_to_values["state"],
            "postal_code": label_to_values["postcode"],
            "country": label_to_values["country"],
        }
        # replace empty strings with None
        return {k: " ".join(v) or None for k, v in renamed.items()}

    return udf(address_string)


def postal_fingerprint_address(address: ir.StructValue) -> ir.ArrayValue:
    """Generate multiple hashes of an address string to be used for e.g. blocking.

    .. note:: To use this function, you need to have the optional `postal` library
    installed.

    This uses the near-dupe hashing functionality of `postal` to expand the root
    of each address component, ignoring tokens such as "road" or "street" in
    street names.

    For street names, whitespace is removed so that for example "Sea Grape Ln" and
    "Seagrape Ln" will both normalize to "seagrape".

    This returns a list of normalized tokens that are the minimum
    required information to represent the given address.

    Near-dupe hashes can be used as keys when blocking,
    to generate pairs of potential duplicates.

    Further details about the hashing function can be found
    [here](https://github.com/openvenues/libpostal/releases/tag/v1.1).

    Note that `postal.near_dupe.near_dupe_hashes` can optionally hash names and
    use latlon coordinates for geohashing, but this function only hashes addresses.
    Name and geo-hashing must be implemented elsewhere

    Examples
    -------
    >>> address = ibis.struct(
    ...     {
    ...         "street1": "123 Main Street",
    ...         "street2": "",
    ...         "city": "Springfield",
    ...         "state": "IL",
    ...         "postal_code": "62701",
    ...         "country": "us",
    ...     }
    ... )
    >>> postal_fingerprint_address(address).execute()  # doctest: +SKIP
    [
        "act|main street|123|springfield",
        "act|main|123|springfield",
        "apc|main street|123|62701",
        "apc|main|123|62701",
    ]

    Parameters
    ----------
    address :
        The address

    Returns
    -------
    address_hashes :
        Hashes of the address.
    """
    with _util.optional_import("postal"):
        from postal.near_dupe import near_dupe_hashes as _hash

    @ibis.udf.scalar.python(signature=((_ADDRESS_SCHEMA,), list[str]))
    def udf(address: dict[str, str] | None) -> list[str] | None:
        # remove once https://github.com/ibis-project/ibis/pull/9625 is fixed
        if address is None:
            return None
        # split street1 into house_number and road
        street1 = address["street1"] or ""
        house, *rest = street1.split(" ", 1)
        contains_digits = _DIGITS_REGEX.match(house) is not None
        parsed = {
            "unit": address["street2"],
            "city": address["city"],
            "state": address["state"],
            "postcode": address["postal_code"],
            "country": address["country"],
        }
        if contains_digits:
            # handle the fact that street1 contains both the house number and the road
            parsed["house_number"] = house
            parsed["road"] = " ".join(rest)
        else:
            parsed["road"] = street1

        parsed = {k: v for k, v in parsed.items() if v}

        if len(parsed) == 0:
            # catch empty strings from invalid addresses
            return []
        return _hash(
            list(parsed.keys()),
            list(parsed.values()),
            address_only_keys=True,
        )

    return udf(address)
