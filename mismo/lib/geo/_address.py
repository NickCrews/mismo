from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util, arrays, block, compare, sets, text
from mismo.lib.geo._latlon import distance_km


class AddressFeatures:
    """A opinionated set of normalized features for a US physical address.

    This is suitable for street addresses in the US, but may need to be
    adapted for other countries.

    Examples
    --------
    >>> address = ibis.literal({
    ...     "street1": "123   Main St ",
    ...     "street2": "Apt 3-b",
    ...     "city": "Springfield",
    ...     "state": "",
    ...     "postal_code": "12345",
    ...     "latitude": 123.456,
    ... })
    >>> features = AddressFeatures(address)

    Whitespace is normed, case is converted to uppercase:

    >>> features.street1.execute()
    '123 MAIN ST'

    Punctuation is removed:

    >>> features.street2.execute()
    'APT 3B'

    Empty strings are converted to NULL:

    >>> features.state.execute() is None
    True

    We add some convenience features:

    >>> features.street_number.execute()
    '123'
    >>> features.street_no_number.execute()
    'MAIN ST'
    >>> features.all_null.execute()
    np.False_

    You can still access the original fields that we didn't normalize:

    >>> features.raw.latitude.execute()
    np.float64(123.456)

    For use in in preparing data for blocking, you can get all the features as a struct:

    >>> features.as_struct().execute()
    {'street1': '123 MAIN ST', 'street2': 'APT 3B', 'city': 'SPRINGFIELD', 'state': None, 'postal_code': '12345', 'street_number': '123', 'street_no_number': 'MAIN ST', 'street_ngrams': ['123 ', 'MAIN', '23 M', 'AIN ', '3 MA', 'IN S', ' MAI', 'N ST', 'APT ', 'PT 3', 'T 3B', '123 MAIN ST', 'APT 3B'], 'latitude': 123.456}
    """  # noqa: E501

    def __init__(self, raw: ir.StructValue | ir.Table, *, street_ngrams_n: int = 4):
        """Create a set of features for an address.

        Assumes the input has already been cleaned with eg a geocoder.
        This only does simple string-based normalization and cleaning,
        eg removing punctuation and converting to uppercase.

        Parameters
        ----------
        raw
            A struct or table with the following fields:
            - street1: string
            - street2: string
            - city: string
            - state: string
            - postal_code: string

            There MAY be additional fields, eg "latitude" and "longitude".
        """
        self.raw = raw
        self.street_ngrams_n = street_ngrams_n

    @property
    def street1(self) -> ir.StringValue:
        """The normalized first line of the street address."""
        return self._norm(self.raw.street1)

    @property
    def street2(self) -> ir.StringValue:
        """The normalized second line of the street address."""
        return self._norm(self.raw.street2)

    @property
    def city(self) -> ir.StringValue:
        """The normalized city."""
        return self._norm(self.raw.city)

    @property
    def state(self) -> ir.StringValue:
        """The normalized state."""
        return self._norm(self.raw.state)

    @property
    def postal_code(self) -> ir.StringValue:
        """The normalized postal code."""
        return self._norm(self.raw.postal_code)

    @property
    def street_number(self) -> ir.StringValue:
        """The street number from street1, eg "123" from "123 Main St"."""
        return self.street1.re_replace(r"^(\d+)\s.*", r"\1")

    @property
    def street_no_number(self) -> ir.StringValue:
        """
        The street1 with the street number removed, eg "Main St" from "123 Main St".
        """
        return self.street1.re_replace(r"^\d+\s+", "")

    @property
    def street_ngrams(self) -> ir.ArrayValue:
        """Ngrams of the normalized street1 and street2. Useful for blocking."""
        return (
            text.ngrams(self.street1.fill_null(""), n=self.street_ngrams_n)
            + text.ngrams(self.street2.fill_null(""), n=self.street_ngrams_n)
            + ibis.array([self.street1, self.street2])
        )

    @property
    def all_null(self) -> ir.BooleanValue:
        """True if all normalized fields are null."""
        return ibis.and_(
            self.street1.isnull(),
            self.street2.isnull(),
            self.city.isnull(),
            self.state.isnull(),
            self.postal_code.isnull(),
        )

    def as_struct(self) -> ir.StructValue:
        """Return the normalized fields as a struct.

        This also includes any fields in the input that are not one of the
        standard address fields.
        For example, if the input has a "latitude" field, that will be included
        in the output.
        """
        d = {
            "street1": self.street1,
            "street2": self.street2,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "street_number": self.street_number,
            "street_no_number": self.street_no_number,
            "street_ngrams": self.street_ngrams,
        }
        fields = (
            self.raw.type().names
            if isinstance(self.raw, ir.StructValue)
            else self.raw.columns
        )
        for field in fields:
            if field not in d:
                d[field] = self.raw[field]
        return ibis.struct(d)

    @staticmethod
    def _norm(s):
        return (
            s.strip()
            .upper()
            .re_replace(r"\s+", " ")  # collapse whitespace
            .re_replace(r"[^0-9A-Z ]", "")
            .nullif("")
        )


class AddressesFeatures:
    def __init__(self, raw: ir.ArrayValue):
        self.raw = raw

    @property
    def all(self):
        def f(address):
            features = AddressFeatures(address)
            return features.all_null.ifelse(ibis.null(), features.as_struct())

        return self.raw.map(f).filter(lambda a: a.notnull())


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
    if "street1_no_number" in left.type().names:
        street_simple_col = "street1_no_number"
    else:
        street_simple_col = "street1"
    cases = [
        ibis.and_(
            left.street1 == right.street1,
            left.street1.length() >= 5,
        ),
        ibis.and_(
            text.damerau_levenshtein_ratio(
                left[street_simple_col], right[street_simple_col]
            )
            > 0.9,
            text.damerau_levenshtein_ratio(left.city, right.city) > 0.9,
        ),
        ibis.and_(
            text.damerau_levenshtein_ratio(
                left[street_simple_col], right[street_simple_col]
            )
            > 0.9,
            # >=.8 so that a transposition in a 5digit zipcode match,
            # eg "12345" and "12354"
            text.damerau_levenshtein_ratio(left.postal_code, right.postal_code) >= 0.8,
        ),
    ]
    if "street1_number" in left.type().names:
        cases.append(
            ibis.and_(
                left.street1_number == right.street1_number,
                text.damerau_levenshtein_ratio(
                    left[street_simple_col], right[street_simple_col]
                )
                > 0.4,
                text.damerau_levenshtein_ratio(left.city, right.city) > 0.9,
            )
        )
    return ibis.or_(*cases)


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


class TokenBlocker:
    def __init__(
        self, tokens_column: str = "addresses_tokens", *, max_records_frac=0.01
    ):
        self.tokens_column = tokens_column
        self.max_records_frac = max_records_frac

    def __call__(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        def f(t):
            return arrays.array_filter_isin_other(
                t,
                t[self.tokens_column],
                sets.rare_terms(
                    t[self.tokens_column], max_records_frac=self.max_records_frac
                ),
                result_format="_addresses_keywords",
            )

        t1 = f(t1)
        t2 = f(t2)
        blocker = block.KeyBlocker(_._addresses_keywords.unnest())
        return blocker(t1, t2, **kwargs)


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
        column_compared: str = "{column}_compared",
    ):
        self.column = column
        self.column_featured = column_featured.format(column=column)
        self.column_compared = column_compared.format(column=column)

    def prepare(self, t: ir.Table) -> ir.Table:
        """Prepares the table for blocking, adding normalized and tokenized columns."""
        addrs: ir.ArrayColumn = t[self.column]
        addrs = addrs.fill_null(ibis.literal([]))

        def featurize(address: ir.StructValue) -> ir.StructValue:
            features = AddressFeatures(address)
            return features.all_null.ifelse(ibis.null(), features.as_struct())

        addresses_featured = addrs.map(featurize).filter(lambda a: a.notnull())
        addresses_tokens = (
            addresses_featured.map(lambda a: a.street_ngrams).flatten().unique()
        )
        t = t.mutate(_addresses_tokens=addresses_tokens)
        t = arrays.array_filter_isin_other(
            t,
            t._addresses_tokens,
            sets.rare_terms(addresses_tokens, max_records_n=500),
            result_format="_addresses_keywords",
        )
        features = ibis.struct(
            {
                "addresses": addresses_featured,
                "addresses_keywords": _._addresses_keywords,
            }
        )
        t = t.mutate(features.name(self.column_featured)).drop(
            "_addresses_tokens", "_addresses_keywords"
        )
        return t

    def block(self, t1: ir.Table, t2: ir.Table, **kwargs) -> ir.Table:
        blocker = block.KeyBlocker(_[self.column_featured.addresses_keywords].unnest())
        return blocker(t1, t2, **kwargs)

    def compare(self, t: ir.Table) -> ir.Table:
        left = t[self.column_featured + "_l"]
        right = t[self.column_featured + "_r"]
        combos = arrays.array_combinations(left, right)
        levels = combos.map(lambda pair: match_level(pair.l, pair.r))
        best = arrays.array_min(levels)
        return t.mutate(best.name(self.column_compared))
