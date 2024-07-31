from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util, arrays, block, compare, sets, text
from mismo.lib.geo._latlon import distance_km


def featurize_address(address: ir.StructValue) -> ir.StructValue:
    """Normalize to uppercase, strip whitespace, and remove punctuation.

    Parameters
    ----------
    address : ir.StructValue
        The address.

    Returns
    -------
    normalized : ir.StructValue
        The normalized address.
    """

    def norm(s):
        return s.strip().upper().re_replace(r"[0-9A-Z\s]", "")

    def drop_street_number(street1: ir.StringValue) -> ir.StringValue:
        return street1.re_replace(r"^\d+\s+", "")

    return ibis.struct(
        {
            "street1": norm(address.street1),
            "street1_no_number": drop_street_number(norm(address.street1)),
            "street2": norm(address.street2),
            "city": norm(address.city),
            "state": norm(address.state),
            "postal_code": norm(address.postal_code),
            # drop country, this is way too broad to be useful for matching
            # "country": norm(address.country),
        }
    )


class AddressesMatchLevel(compare.MatchLevel):
    """How closely two addresses match."""

    STREET1_AND_CITY_OR_POSTAL = 0
    """The street1, city, and state match."""
    POSSIBLE_TYPO = 1
    """If you consider typos, the addresses match.
    
    eg the levenstein distance is below a certain threshold.
    """
    SAME_REGION = 2
    """The postal code, or city and state, match."""
    WITHIN_100KM = 3
    """The addresses are within 100 km of each other."""
    SAME_STATE = 4
    """The states match."""
    NULL = 5
    """At least one street1, city, or state is NULL from either side."""
    ELSE = 6
    """None of the above."""


def match_level(left: ir.StructValue, right: ir.StructValue) -> AddressesMatchLevel:
    """Compare two address structs, and return the match level."""
    if "latitude" in left.type().names:
        within_100km_levels = [
            (
                distance_km(
                    lat1=left.latitude,
                    lon1=left.longitude,
                    lat2=right.latitude,
                    lon2=right.longitude,
                )
                <= 100,
                AddressesMatchLevel.WITHIN_100KM.as_integer(),
            ),
        ]
    else:
        within_100km_levels = []

    if "street1_no_number" in left.type().names:
        street_simple_col = "street1_no_number"
    else:
        street_simple_col = "street1"
    return _util.cases(
        (
            ibis.or_(
                _util.struct_isnull(
                    left, how="any", fields=["street1", "city", "state"]
                ),
                _util.struct_isnull(
                    right, how="any", fields=["street1", "city", "state"]
                ),
            ),
            AddressesMatchLevel.NULL.as_integer(),
        ),
        (
            ibis.and_(
                left.street1 == right.street1,
                ibis.or_(
                    left.city == right.city, left.postal_code == right.postal_code
                ),
            ),
            AddressesMatchLevel.STREET1_AND_CITY_OR_POSTAL.as_integer(),
        ),
        (
            ibis.and_(
                text.damerau_levenshtein_ratio(
                    left[street_simple_col], right[street_simple_col]
                )
                > 0.9,
                ibis.or_(
                    text.damerau_levenshtein_ratio(left.city, right.city) > 0.9,
                    # >=.8 so that a transposition in a 5digit zipcodem match,
                    # eg "12345" and "12354"
                    text.damerau_levenshtein_ratio(left.postal_code, right.postal_code)
                    >= 0.8,
                ),
            ),
            AddressesMatchLevel.POSSIBLE_TYPO.as_integer(),
        ),
        (
            ibis.or_(
                left.postal_code == right.postal_code,
                ibis.and_(left.city == right.city, left.state == right.state),
            ),
            AddressesMatchLevel.SAME_REGION.as_integer(),
        ),
        *within_100km_levels,
        (left.state == right.state, AddressesMatchLevel.SAME_STATE.as_integer()),
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
        column_featured: str = "{column}_featured",
        column_tokens: str = "{column}_tokens",
        column_keywords: str = "{column}_keywords",
        column_compared: str = "{column}_compared",
    ):
        self.column = column
        self.column_featured = column_featured.format(column=column)
        self.column_tokens = column_tokens.format(column=column)
        self.column_keywords = column_keywords.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prepare(self, t: ir.Table) -> ir.Table:
        """Prepares the table for blocking, adding normalized and tokenized columns."""
        addrs = t[self.column]
        t = t.mutate(addrs.map(featurize_address).name(self.column_featured))
        t = t.mutate(
            t[self.column_featured]
            .map(lambda address: _util.struct_tokens(address, unique=False))
            .flatten()
            .name("_tokens_nonunique")
        )
        # Array.unique() results in 4 duplications of the input, so .cache it so
        # we only execute it once. See https://github.com/ibis-project/ibis/issues/8770
        t = t.cache()
        t = t.mutate(t._tokens_nonunique.unique().name(self.column_tokens)).drop(
            "_tokens_nonunique"
        )
        t = arrays.array_filter_isin_other(
            t,
            self.column_tokens,
            sets.rare_terms(t[self.column_tokens], max_records_frac=0.01),
            result_format=self.column_keywords,
        )
        return t

    def block(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        blocker = block.KeyBlocker(_[self.column_keywords].unnest())
        return blocker(t1, t2, **kwargs)

    def compare(self, t: ir.Table) -> ir.Table:
        left = t[self.column_featured + "_l"]
        right = t[self.column_featured + "_r"]
        combos = arrays.array_combinations(left, right)
        levels = combos.map(lambda pair: match_level(pair.l, pair.r))
        best = arrays.array_min(levels)
        return t.mutate(best.name(self.column_compared))
