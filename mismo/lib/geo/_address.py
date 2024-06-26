from __future__ import annotations

import ibis
from ibis.expr import types as ir

from mismo import _util, arrays
from mismo.block import MinhashLshBlocker
from mismo.compare import MatchLevel
from mismo.lib.geo._latlon import distance_km
from mismo.sets import rare_terms


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


class AddressesMatchLevel(MatchLevel):
    """How closely two addresses match."""

    NULL = 0
    """At least one street1, city, or state is NULL from either side."""
    STREET1_CITY = 1
    """The street1 and city match."""
    STREET1_AND_CITY_OR_POSTAL = 2
    """The street1, city, and state match."""
    SAME_REGION = 3
    """The postal code, or city and state, match."""
    WITHIN_100KM = 4
    """The addresses are within 100 km of each other."""
    SAME_STATE = 5
    """The states match."""
    ELSE = 6
    """None of the above."""


def best_match(left: ir.ArrayValue, right: ir.ArrayValue) -> AddressesMatchLevel:
    """Compare two arrays of address structs, and return the best match level.

    We compare every pair of addresses, and whichever pair has the highest match
    level, that is the match level for the two arrays.

    Parameters
    ----------
    left :
        The first set of addresses.
    right :
        The second set of addresses.

    Returns
    -------
    level :
        The match level.
    """
    combos = arrays.array_combinations(left, right)
    if "latitude" in left.type().value_type.names:
        within_100km_levels = [
            (
                arrays.array_min(
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
                AddressesMatchLevel.WITHIN_100KM.as_integer(),
            ),
        ]
    else:
        within_100km_levels = []
    return _util.cases(
        (
            arrays.array_all(
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
            AddressesMatchLevel.NULL.as_integer(),
        ),
        (
            arrays.array_any(
                combos.map(lambda pair: same_address_for_mailing(pair.l, pair.r))
            ),
            AddressesMatchLevel.STREET1_AND_CITY_OR_POSTAL.as_integer(),
        ),
        (
            arrays.array_any(combos.map(lambda pair: same_region(pair.l, pair.r))),
            AddressesMatchLevel.SAME_REGION.as_integer(),
        ),
        *within_100km_levels,
        (
            arrays.array_any(combos.map(lambda pair: pair.l.state == pair.r.state)),
            AddressesMatchLevel.SAME_STATE.as_integer(),
        ),
        else_=AddressesMatchLevel.ELSE.as_integer(),
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
        column_compared: str = "{column}_compared",
    ):
        self.column = column
        self.column_normed = column_normed.format(column=column)
        self.column_tokens = column_tokens.format(column=column)
        self.column_keywords = column_keywords.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prepare(self, t: ir.Table) -> ir.Table:
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
        t = arrays.array_filter_isin_other(
            t,
            self.column_tokens,
            rare_terms(t[self.column_tokens], max_records_frac=0.01),
            result_format=self.column_keywords,
        )
        return t

    def block(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        blocker = MinhashLshBlocker(
            terms_column=self.column_keywords, band_size=10, n_bands=10
        )
        return blocker(t1, t2, **kwargs)

    def compare(self, t: ir.Table) -> ir.Table:
        al = t[self.column_normed + "_l"]
        ar = t[self.column_normed + "_r"]
        return t.mutate(best_match(al, ar).name(self.column_compared))


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
