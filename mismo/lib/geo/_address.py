from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _structs, _util, arrays, compare, sets, text
from mismo.lib.geo._latlon import distance_km
from mismo.lib.geo._regex_parse import parse_street1_re


def _featurize_many(
    t: ibis.Table, *, input_column: str, output_column: str
) -> ibis.Table:
    """Given a table with column addresses:Array<Struct>, add a column address_featured:Array<Struct>."""  # noqa: E501
    with_id = t.mutate(__id=ibis.row_number())
    unnested = with_id.select("__id", _[input_column].unnest().name(input_column))
    lookup = (
        _featurize(
            unnested,
            input_column=input_column,
            output_name="address_featured",
        )
        .group_by("__id")
        .agg(_.address_featured.collect().name(output_column))
    )
    lookup = lookup.cache()
    rejoined = _util.join_lookup(
        with_id, lookup, "__id", defaults={output_column: []}
    ).drop("__id")
    rejoined = rejoined.mutate(
        _[input_column].isnull().ifelse(None, _[output_column]).name(output_column)
    )
    return rejoined


def _norm_field(s: ir.StringValue) -> ir.StringValue:
    return (
        s.strip()
        .upper()
        .re_replace(r"\s+", " ")  # collapse whitespace
        .re_replace(r"[^0-9A-Z\.\- ]", "")
        .nullif("")
    )


def _norm_address(address: ir.StructValue) -> ir.StructValue:
    return ibis.struct(
        {
            "street1": _norm_field(address.street1),
            "street2": _norm_field(address.street2),
            "city": _norm_field(address.city),
            "state": _norm_field(address.state),
            "postal_code": _norm_field(address.postal_code),
        }
    )


def _featurize(
    t: ibis.Table, *, input_column: str | ir.StructValue, output_name: str
) -> ibis.Table:
    """Add a StructColumn named "address_featured" to the table."""
    # we have this Table -> Table API for performance reasons:
    # by doing all these operations in a much of sequential .mutate()s,
    # it compiles to a bunch of chained CTEs in the SQL.
    #
    # If we only did one big .mutate(), some of the deep subexpressions would
    # get copy-pasted many many times (eg Array.unique() results in the argument
    # being copy-pasted 4 times: # https://github.com/ibis-project/ibis/blob/75ecf038d917b968dcc493c69429abe1e8549dd2/ibis/backends/sql/compilers/duckdb.py#L145-L155).
    # If duckdb/the backend were clever with common subexpression elimination,
    # this might not be a problem. But, duckdb appears to evaluate a regex
    # for every time it appears in the SQL. See https://github.com/duckdb/duckdb/discussions/14649.
    # So, if we did one .mutate(), we would end up with like literally 100 regex
    # evaluations in the SQL, which is 100x slower than evaluating the regex once.
    input_column = t.bind(input_column)[0]
    t = t.mutate(_parsed=parse_street1_re(input_column.street1), _cleaned=input_column)
    t = t.mutate(
        __address_featured=ibis.struct(
            dict(
                street1=_._cleaned.street1,
                city=_._cleaned.city,
                postal_code=_._cleaned.postal_code,
                state=_._cleaned.state,
                street_name=_norm_field(_._parsed.StreetName),
                # either 123 from "123 Main St" or "PO BOX 123". Only one of these
                # will be non-empty.
                street_number=_norm_field(
                    _._parsed.AddressNumber + "" + _._parsed.USPSBoxID
                ),
                is_pobox=_._parsed.USPSBoxID != "",
            )
        )
    )
    t = t.mutate(
        __address_featured=_structs.mutate(
            t.__address_featured,
            street_number_sorted=t.__address_featured.street_number.split("")
            .sort()
            .join(""),
        )
    )
    t = t.mutate(
        _structs.mutate(
            t.__address_featured,
            street_trigrams=text.ngrams(t._cleaned.street1, 3).unique(),
        ).name(output_name)
    ).drop(
        # "_parsed",
        "__address_featured",
    )
    return t


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
    ELSE = 6
    """None of the above."""


def _is_possible_typo(left: ir.StructValue, right: ir.StructValue) -> ir.BooleanValue:
    cases = [
        ibis.and_(
            left.street1 == right.street1,
            left.street1.length() >= 5,
        ),
        ibis.and_(
            text.damerau_levenshtein_ratio(left.street_name, right.street_name) > 0.9,
            text.damerau_levenshtein_ratio(left.city, right.city) > 0.9,
        ),
        ibis.and_(
            text.damerau_levenshtein_ratio(left.street_name, right.street_name) > 0.9,
            # >=.8 so that a transposition in a 5digit zipcode match,
            # eg "12345" and "12354"
            text.damerau_levenshtein_ratio(left.postal_code, right.postal_code) >= 0.8,
        ),
        ibis.and_(
            left.street_number == right.street_number,
            text.damerau_levenshtein_ratio(left.street_name, right.street_name) > 0.4,
            text.damerau_levenshtein_ratio(left.city, right.city) > 0.9,
        ),
    ]
    return ibis.or_(*cases)


def match_level(left: ir.StructValue, right: ir.StructValue) -> ir.IntegerValue:
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

    return _util.cases(
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
            _is_possible_typo(left, right),
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
        column_cleaned: str = "{column}_cleaned",
        column_featured: str = "{column}_featured",
        column_compared: str = "{column}_compared",
    ):
        self.column = column
        self.column_cleaned = column_cleaned.format(column=column)
        self.column_featured = column_featured.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prepare_for_fast_linking(self, t: ir.Table) -> ir.Table:
        """Prepares the table for fast linking, adding a normalized column."""
        t = t.mutate(t[self.column].map(_norm_address).name(self.column_cleaned))
        return t

    def prepare_for_blocking(self, t: ir.Table) -> ir.Table:
        """Prepares the table for blocking, adding normalized and tokenized columns."""
        t = _featurize_many(
            t, input_column=self.column_cleaned, output_column=self.column_featured
        )
        t = t.cache()
        t = t.mutate(
            _addresses_tokens=_.addresses_featured.map(lambda a: a.street_trigrams)
            .flatten()
            .unique()
        )
        t = t.cache()
        t = arrays.array_filter_isin_other(
            t,
            t._addresses_tokens,
            sets.rare_terms(t._addresses_tokens, max_records_n=500),
            result_format="_addresses_keywords",
        )
        t = t.cache()
        t = t.mutate(
            ibis.struct(
                {
                    "addresses": t.addresses_featured,
                    "addresses_keywords": _._addresses_keywords,
                }
            ).name(self.column_featured)
        ).drop("_addresses_tokens", "_addresses_keywords")
        t = t.cache()
        return t

    def compare(self, t: ir.Table) -> ir.Table:
        left = t[self.column_featured + "_l"].addresses
        right = t[self.column_featured + "_r"].addresses
        combos = arrays.array_combinations(left, right)
        levels = combos.map(lambda pair: match_level(pair.l, pair.r))
        best = arrays.array_min(levels)
        return t.mutate(best.name(self.column_compared))
