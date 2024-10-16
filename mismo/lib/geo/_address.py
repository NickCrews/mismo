from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _util, arrays, block, compare, sets, text
from mismo.lib.geo._latlon import distance_km
from mismo.lib.geo._spacy import TaggedAddress


class AddressFeatures:
    """A opinionated set of normalized features for a US mailing address.

    This is suitable for street addresses in the US, but may need to be
    adapted for other countries.

    Examples
    --------
    >>> address = ibis.literal({
    ...     "street1": "132   Main St ",
    ...     "street2": "Apt 3-b",
    ...     "city": "Springfield",
    ...     "state": "",
    ...     "postal_code": "12345",
    ...     "latitude": 123.456,
    ... })
    >>> features = AddressFeatures(address)

    Whitespace is normed, case is converted to uppercase:

    >>> features.street1.execute()
    '132 MAIN ST'

    Punctuation is removed:

    >>> features.street2.execute()
    'APT 3B'

    Empty strings are converted to NULL:

    >>> features.state.execute() is None
    True

    We add some convenience features:

    >>> features.street_number.execute()
    '132'
    >>> features.street_number_sorted.execute()
    '123'
    >>> features.street_name.execute()
    'MAIN'
    >>> features.all_null.execute()
    np.False_

    You can still access the original fields that we didn't normalize:

    >>> features.raw.latitude.execute()
    np.float64(123.456)

    For use in in preparing data for blocking, you can get all the features as a struct:

    >>> features.as_struct().execute()
    {'street1': '132 MAIN ST', 'street2': 'APT 3B', 'city': 'SPRINGFIELD', 'state': None, 'postal_code': '12345', 'street_number': '132', 'street_name': 'MAIN', 'street_ngrams': ['132 ', 'MAIN', '32 M', 'AIN ', '2 MA', 'IN S', ' MAI', 'N ST', 'APT ', 'PT 3', 'T 3B', '132 MAIN ST', 'APT 3B'], 'latitude': 123.456}
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
    def _tagged(self) -> TaggedAddress:
        # We only use the street name and address number, so only pass in the street1.
        # This is both more performant, and it removes the possibility of
        # "1-b" in "apt 1-b" being tagged as a street number (which I just saw happen).
        return TaggedAddress.from_oneline(self.street1)

    @property
    def street_name(self) -> ir.StringValue:
        """
        The normalized street name from street1, eg "OAK TREE" from "123 oak  tree St".
        """
        return self._norm(self._tagged.StreetName)

    @property
    def street_number(self) -> ir.StringValue:
        """The normalized street number from street1, eg "132" from "132 Main St"."""
        return self._norm(self._tagged.AddressNumber)

    @property
    def street_number_sorted(self) -> ir.StringValue:
        """
        The sorted normalized street number from street1, eg "123" from "132 Main St".

        Useful to account for typos in the street number.
        """
        return self.street_number.split("").sort().join("")

    @property
    def street_ngrams(self) -> ir.ArrayValue:
        """Ngrams of the self.street_number and street_name. Useful for blocking."""
        return (
            text.ngrams(self.street_number_sorted.fill_null(""), n=self.street_ngrams_n)
            + text.ngrams(self.street_name.fill_null(""), n=self.street_ngrams_n)
            + ibis.array([self.street_number_sorted, self.street_name])
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
            "taggings": self._tagged.taggings,
            "street_number": self.street_number,
            "street_number_sorted": self.street_number_sorted,
            "street_name": self.street_name,
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

        t = t.mutate(
            _addresses_featured=addrs.map(featurize).filter(lambda a: a.notnull())
        )
        t = t.mutate(
            _addresses_tokens=_._addresses_featured.map(lambda a: a.street_ngrams)
            .flatten()
            .unique()
        )
        t = arrays.array_filter_isin_other(
            t,
            t._addresses_tokens,
            sets.rare_terms(t._addresses_tokens, max_records_n=500),
            result_format="_addresses_keywords",
        )
        t = t.mutate(
            ibis.struct(
                {
                    "addresses": t._addresses_featured,
                    "addresses_keywords": _._addresses_keywords,
                }
            ).name(self.column_featured)
        ).drop("_addresses_featured", "_addresses_tokens", "_addresses_keywords")
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
