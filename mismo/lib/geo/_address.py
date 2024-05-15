from __future__ import annotations

from collections import defaultdict

import ibis
from ibis.expr import types as ir

from mismo import _array, _util
from mismo.compare import LevelComparer, compare
from mismo.lib.geo._latlon import distance_km
from mismo.sets import rare_terms

ADDRESS_SCHEMA = """struct<street1:string, 
                           street2:string, 
                           city:string, 
                           state:string, 
                           postal_code:string, 
                           country:string>"""


def _create_expand_schema():
    """Creates the schema used to store expanded address components."""
    components = [
        "house_number",
        "street",
        "unit",
        "city",
        "state",
        "postcode",
        "country",
    ]
    c_struct = "struct<component:array<string>, root:array<string>>"
    struct_list = ", ".join([f"{c}:{c_struct}" for c in components])
    return f"struct<{struct_list}>"


EXPAND_SCHEMA = _create_expand_schema()


def same_region(
    address1: ir.StructColumn,
    address2: ir.StructColumn,
) -> ir.BooleanColumn:
    """Exact match on postal code, or city and state.

    Parameters
    ----------
    address1 : ir.StringColumn
        The first address.
    address2 : ir.StringColumn
        The second address.

    Returns
    -------
    same : ir.BooleanColumn
        Whether the two addresses are in the same region.
    """
    return ibis.or_(
        address1.postal_code == address2.postal_code,
        ibis.and_(address1.city == address2.city, address1.state == address2.state),
    )


def same_address_for_mailing(
    address1: ir.StructColumn,
    address2: ir.StructColumn,
) -> ir.BooleanColumn:
    """Exact match on street1, and either city or postal code.

    Parameters
    ----------
    address1 : ir.StringColumn
        The first address.
    address2 : ir.StringColumn
        The second address.

    Returns
    -------
    same : ir.BooleanColumn
        Whether the two addresses are the same.
    """
    return ibis.and_(
        address1.street1 == address2.street1,
        ibis.or_(
            address1.city == address2.city, address1.postal_code == address2.postal_code
        ),
    )


def normalize_address(address: ir.StructValue) -> ir.StructValue:
    """Normalize an address to uppercase, and remove leading and trailing whitespace.

    Parameters
    ----------
    address : ir.StructValue
        The address.

    Returns
    -------
    normalized : ir.StructValue
        The normalized address.
    """
    return ibis.struct(
        {
            "street1": address.street1.upper().strip(),
            "street2": address.street2.upper().strip(),
            "city": address.city.upper().strip(),
            "state": address.state.upper().strip(),
            "postal_code": address.postal_code.upper().strip(),
            "country": address.country.upper().strip(),
        }
    )


class AddressesDimension:
    """Preps, blocks, and compares based on array<address> columns.

    An address is a Struct of the type
    `struct<
        street1: string,
        street2: string,  # eg "Apt 3"
        city: string,
        state: string,
        postal_code: string,  # zipcode in the US
        country: string,
    >`.
    This operates on columns of type `array<address>`. In other words,
    it is useful for comparing eg people, who might have multiple addresses,
    and they are the same person if any of their addresses match.
    """

    def __init__(
        self,
        column: str,
        *,
        column_normed: str = "{column}_normed",
        column_tokens: str = "{column}_tokens",
        column_keywords: str = "{column}_keywords",
    ):
        self.column = column
        self.column_normed = column_normed.format(column=column)
        self.column_tokens = column_tokens.format(column=column)
        self.column_keywords = column_keywords.format(column=column)

    def prep(self, t: ir.Table) -> ir.Table:
        """Prepares the table for blocking, adding normalized and tokenized columns."""
        addrs = t[self.column]
        t = t.mutate(addrs.map(normalize_address).name(self.column_normed))
        tokens_nonunique = (
            t[self.column_normed]
            .map(lambda address: address_tokens(address, unique=False))
            .flatten()
        )
        t = t.mutate(_tokens_nonunique=tokens_nonunique)
        # Array.unique() results in 4 duplications of the input, so .cache it so
        # we only execute it once. See https://github.com/ibis-project/ibis/issues/8770
        t = t.cache()
        t = t.mutate(t._tokens_nonunique.unique().name(self.column_tokens)).drop(
            "_tokens_nonunique"
        )
        t = _array.array_filter_isin_other(
            t,
            self.column_tokens,
            rare_terms(t[self.column_tokens], max_records_frac=0.01),
            result_format=self.column_keywords,
        )
        return t

    def compare(self, t: ir.Table) -> ir.Table:
        al = t[self.column_normed + "_l"]
        ar = t[self.column_normed + "_r"]
        combos = _array.array_combinations(al, ar)
        if "latitude" in al.type().value_type.names:
            within_100km_levels = [
                (
                    "within_100km",
                    _array.array_min(
                        combos.map(
                            lambda pair: distance_km(
                                lat1=pair.l.latitude,
                                lon1=pair.l.longitude,
                                lat2=pair.r.latitude,
                                lon2=pair.r.longitude,
                            )
                        )
                    )
                    <= 100,
                ),
            ]
        else:
            within_100km_levels = []
        levels = [
            (
                "null",
                _array.array_all(
                    combos.map(
                        lambda pair: ibis.or_(
                            _util.struct_isnull(
                                pair.l, how="any", fields=["street1", "city", "state"]
                            ),
                            _util.struct_isnull(
                                pair.r, how="any", fields=["street1", "city", "state"]
                            ),
                        )
                    )
                ),
            ),
            (
                "street1_and_city_or_postal_code",
                _array.array_any(
                    combos.map(lambda pair: same_address_for_mailing(pair.l, pair.r))
                ),
            ),
            (
                "same_region",
                _array.array_any(combos.map(lambda pair: same_region(pair.l, pair.r))),
            ),
            *within_100km_levels,
            (
                "same_state",
                _array.array_any(combos.map(lambda pair: pair.l.state == pair.r.state)),
            ),
        ]
        name = type(self).__name__
        return compare(t, LevelComparer(name, levels))


def address_tokens(address: ir.StructValue, *, unique: bool = True) -> ir.ArrayColumn:
    """Extract keywords from an address.

    Parameters
    ----------
    address :
        The address.

    Returns
    -------
    keywords :
        The keywords in the address.
    """
    return _util.struct_tokens(address, unique=unique)


@ibis.udf.scalar.python(signature=(("string",), ADDRESS_SCHEMA))
def parse_address(address_string: str):
    """Parse individual fields from an address string.
    This uses the optional `postal` library to extract individual fields
    from the string using the following mapping:

    - house_number + road -> street1
    - unit -> street2
    - city -> city
    - state -> state
    - postcode -> postal_code
    - country -> country

    Any additional fields parsed by postal will not be included

    Parameters
    ----------
    address_string :
        The address as a single string

    Returns
    -------
    address :
        The address.
    """

    with _util.optional_import("postal"):
        from postal.parser import parse_address as _parse_address

    parsed_fields = _parse_address(address_string)
    address_dict = defaultdict(list)
    for k, v in parsed_fields:
        address_dict[v].append(k)
    address_dict = {k: " ".join(v) for k, v in address_dict.items()}
    return {
        "street1": (
            address_dict.get("house_number", "") + " " + address_dict.get("road", "")
        ).strip(),
        "street2": address_dict.get("unit", ""),
        "city": address_dict.get("city", ""),
        "state": address_dict.get("state", ""),
        "postal_code": address_dict.get("postcode", ""),
        "country": address_dict.get("country", ""),
    }


@ibis.udf.scalar.python
def hash_address(address_string: str, address_only_keys: bool = True) -> list[str]:
    """Hash an address string using `postal.near_dupe_hashes`.
    This returns a list of normalized tokens that represent the minimum
    required information to represent the given address.

    Near-dupe hashes can be used as keys when blocking,
    to generate pairs of potential duplicates.

    Parameters
    ----------
    address_string :
        The address as a single string
    address_only_keys :
        If True, only the address fields are used to generate the hash.

    Returns
    -------
    hash :
        The hash of the address.
    """

    with _util.optional_import("postal"):
        from postal.near_dupe import near_dupe_hashes as _hash
        from postal.parser import parse_address as _parse_address
    parsed = dict(_parse_address(address_string))
    if len(parsed) == 0:
      # catch empty strings from invalid address strings
      return []
    return _hash(
        list(parsed.values()), list(parsed.keys()), address_only_keys=address_only_keys
    )


@ibis.udf.scalar.python(signature=(("string",), EXPAND_SCHEMA))
def expand_address_components(address_string: str) -> dict:
    """Parse an address string and expand the components along with their roots
    for subsequent comparison.

    This uses the `postal` library to parse the address string and expand the components

    After parsing the address, each component is expanded into normalized forms,
    along with their roots for the following fields:

    - house_number
    - road (renamed to street)
    - unit
    - city
    - state
    - postcode
    - country

    These expanded components are used by `compare_addresses`
    to evaluate the similarity between two addresses.

    In contrast to the functions within `postal.dedupe`, expanding first and then
    comparing the components and roots is more efficient when applied to
    vectors of addresses.

    This enables the comparison to be performed in O(n+m) time,
    rather than O(n*m) time using the `postal.dedupe`

    Parameters
    ----------
    address_string :
        The address as a single string

    Returns
    -------
    address_components :
        The expanded components and roots for each field.
    """
    with _util.optional_import("postal"):
        from postal.expand import (
            ADDRESS_ANY,
            ADDRESS_HOUSE_NUMBER,
            ADDRESS_NAME,
            ADDRESS_POSTAL_CODE,
            ADDRESS_STREET,
            ADDRESS_UNIT,
            expand_address,
            expand_address_root,
        )
        from postal.parser import parse_address as _parse_address
    parsed = {v: k for k, v in dict(_parse_address(address_string)).items()}
    component_mapping = {
        "house_number": ADDRESS_HOUSE_NUMBER | ADDRESS_ANY,
        "road": ADDRESS_STREET | ADDRESS_ANY,
        "unit": ADDRESS_UNIT | ADDRESS_ANY,
        "city": ADDRESS_NAME | ADDRESS_ANY,
        "state": ADDRESS_NAME | ADDRESS_ANY,
        "postcode": ADDRESS_POSTAL_CODE | ADDRESS_ANY,
        "country": ADDRESS_NAME | ADDRESS_ANY,
    }
    level_rename = {"road": "street"}
    result = {}
    for level, address_components in component_mapping.items():
        component = parsed.get(level, "")
        expanded = expand_address(component, address_components=address_components)
        expanded_root = expand_address_root(
            component, address_components=address_components
        )
        name = level_rename.get(level, level)
        result[name] = {"component": expanded, "root": expanded_root}
    return result


def _is_duplicate(
    value1: ir.StructColumn,
    value2: ir.StructColumn,
    root_comparison_first: bool = True,
    root_comparison_status: str = "EXACT_DUPLICATE",
) -> ir.StringColumn:
    """A helper function to determine if two values are duplicates.

    If `root_comparison_first` is True, return `root_comparison_status` if the root of
    the two values intersect.
    If they don't, check the components and return 'EXACT_DUPLICATE' if they intersect

    Otherwise, return 'EXACT_DUPLICATE' if the components intersect or return
    `root_comparison_status` if the roots intersect.

    'NON_DUPLICATE' is returned if no intersections exist unless both components are
    empty arrays, in which case 'NULL_DUPLICATE' is returned.
    """
    root_match = value1.root.intersect(value2.root).length() > 0
    component_match = value1.component.intersect(value2.component).length() > 0
    null_match = (value1.component.length() == 0) & (value2.component.length() == 0)
    root_comparison = (
        ibis.case().when(root_match, root_comparison_status).else_(None).end()
    )
    component_comparison = (
        ibis.case().when(component_match, "EXACT_DUPLICATE").else_(None).end()
    )
    null_comparison = ibis.case().when(null_match, "NULL_DUPLICATE").else_(None).end()
    if root_comparison_first:
        return ibis.coalesce(
            root_comparison, component_comparison, null_comparison, "NON_DUPLICATE"
        )
    else:
        return ibis.coalesce(
            component_comparison, root_comparison, null_comparison, "NON_DUPLICATE"
        )


def compare_addresses(
    address_components1: ir.StructColumn, address_components2: ir.StructColumn
) -> ir.StructColumn:
    """Compare individual address fields to evaluate their similarity.

    This follows the algorithm used in `postal.dedupe` library to determine
    the match level.

    It is re-implemented here so that it can be efficiently applied to vectors of
    expanded address components

    - house_number/unit/ postcode:
        Compare roots first, return EXACT_DUPLICATE if a match exists
    - street/city/state:
        Compare components first, return NEEDS_REVIEW if only a root match exists

    If no match is found, return NULL_DUPLICATE if both components are null

    Parameters
    ----------

    address_components1 :
        The expanded components and roots of each field for the first address

    address2:
        The expanded components and roots of each field for the second address

    Returns
    -------
    address_comparison:
        A struct that contains the duplicate level for each field
    """
    house_number_match = _is_duplicate(
        address_components1.house_number,
        address_components2.house_number,
        root_comparison_first=True,
        root_comparison_status="EXACT_DUPLICATE",
    )

    street_match = _is_duplicate(
        address_components1.street,
        address_components2.street,
        root_comparison_first=False,
        root_comparison_status="NEEDS_REVIEW",
    )

    unit_match = _is_duplicate(
        address_components1.unit,
        address_components2.unit,
        root_comparison_first=True,
        root_comparison_status="EXACT_DUPLICATE",
    )

    city_match = _is_duplicate(
        address_components1.city,
        address_components2.city,
        root_comparison_first=False,
        root_comparison_status="NEEDS_REVIEW",
    )

    state_match = _is_duplicate(
        address_components1.state,
        address_components2.state,
        root_comparison_first=False,
        root_comparison_status="NEEDS_REVIEW",
    )

    postal_code_match = _is_duplicate(
        address_components1.postcode,
        address_components2.postcode,
        root_comparison_first=True,
        root_comparison_status="EXACT_DUPLICATE",
    )

    country_match = _is_duplicate(
        address_components1.country,
        address_components2.country,
        root_comparison_first=False,
        root_comparison_status="NEEDS_REVIEW",
    )

    return ibis.struct(
        {
            "house_number": house_number_match,
            "street": street_match,
            "unit": unit_match,
            "city": city_match,
            "state": state_match,
            "postcode": postal_code_match,
            "country": country_match,
        }
    )
