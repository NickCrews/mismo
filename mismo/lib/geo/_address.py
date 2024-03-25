from __future__ import annotations

import ibis
from ibis.expr import types as it

from mismo import _util
from mismo.compare import LevelComparer, compare
from mismo.lib.geo._latlon import distance_km
from mismo.text import rare_terms


def same_region(
    address1: it.StructColumn,
    address2: it.StructColumn,
) -> it.BooleanColumn:
    """Exact match on postal code, or city and state.

    Parameters
    ----------
    address1 : it.StringColumn
        The first address.
    address2 : it.StringColumn
        The second address.

    Returns
    -------
    same : it.BooleanColumn
        Whether the two addresses are in the same region.
    """
    return ibis.or_(
        address1.postal_code == address2.postal_code,
        ibis.and_(address1.city == address2.city, address1.state == address2.state),
    )


def same_address_for_mailing(
    address1: it.StructColumn,
    address2: it.StructColumn,
) -> it.BooleanColumn:
    """Exact match on street1, and either city or postal code.

    Parameters
    ----------
    address1 : it.StringColumn
        The first address.
    address2 : it.StringColumn
        The second address.

    Returns
    -------
    same : it.BooleanColumn
        Whether the two addresses are the same.
    """
    return ibis.and_(
        address1.street1 == address2.street1,
        ibis.or_(
            address1.city == address2.city, address1.postal_code == address2.postal_code
        ),
    )


def normalize_address(address: it.StructValue) -> it.StructValue:
    """Normalize an address to uppercase, and remove leading and trailing whitespace.

    Parameters
    ----------
    address : it.StructValue
        The address.

    Returns
    -------
    normalized : it.StructValue
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

    def prep(self, t: it.Table) -> it.Table:
        """Prepares the table for blocking, adding normalized and tokenized columns."""
        addrs = t[self.column]
        addrs_normed = addrs.map(normalize_address)
        t = t.mutate({self.column_normed: addrs_normed})
        tokens = (
            t[self.column_normed]
            .map(lambda address: address_tokens(address))
            .flatten()
            .unique()
        )
        t = t.mutate({self.column_tokens: tokens})
        # since we need the .execute below, might as well .cache()
        # here to avoid re-execution
        t = t.cache()
        rt = rare_terms(t[self.column_tokens], max_records_frac=0.01).term
        # need this to avoid https://stackoverflow.com/questions/77559936/how-to-implementlist-filterarray-elem-elem-in-column-in-other-table
        # see https://github.com/NickCrews/mismo/issues/32
        rt = rt.execute()
        keywords = t[self.column_tokens].filter(lambda tok: tok.isin(rt))
        t = t.mutate({self.column_keywords: keywords})
        return t

    def compare(self, t: it.Table) -> it.Table:
        al = t[self.column_normed + "_l"]
        ar = t[self.column_normed + "_r"]
        if "latitude" in al.type().names:
            within_100km_levels = [
                (
                    "within_100km",
                    distance_km(
                        lat1=al.latitude,
                        lon1=al.longitude,
                        lat2=ar.latitude,
                        lon2=ar.longitude,
                    )
                    <= 100,
                ),
            ]
        else:
            within_100km_levels = []
        levels = [
            (
                "null",
                ibis.or_(
                    _util.struct_isnull(
                        al, how="any", fields=["street1", "city", "state"]
                    ),
                    _util.struct_isnull(
                        ar, how="any", fields=["street1", "city", "state"]
                    ),
                ),
            ),
            ("street1_and_city_or_postal_code", same_address_for_mailing(al, ar)),
            ("same_region", same_region(al, ar)),
            *within_100km_levels,
            ("same_state", al.state == ar.state),
        ]
        name = type(self).__name__
        return compare(t, LevelComparer(name, levels))


def address_tokens(address: it.StructValue, *, unique: bool = True) -> it.ArrayColumn:
    """Extract keywords from an address.

    Parameters
    ----------
    address : it.StructValue
        The address.

    Returns
    -------
    keywords : it.ArrayColumn
        The keywords in the address.
    """
    return _util.struct_tokens(address, unique=unique)
