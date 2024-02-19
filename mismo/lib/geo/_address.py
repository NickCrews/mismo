from __future__ import annotations

import dataclasses
from typing import Callable

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo import _util
from mismo.compare import LevelComparer, compare


@dataclasses.dataclass
class AddressLevelComparer:
    """An opinionated [LevelComparer][mismo.compare.LevelComparer] for street addresses.

    This labels record pairs with a level of similarity based on how well
    the addresses match.

    This assumes that you have already normalized the addresses,
    eg you have already transformed all of
    "New York", "NY", and  " NEW YORK" to the same value.
    """

    def __init__(
        self,
        column_left: str | Deferred | Callable[[it.Table], it.StructColumn],
        column_right: str | Deferred | Callable[[it.Table], it.StructColumn],
        name: str = "address_agreement",
    ):
        """Compare two tables on the specified address columns.

        An address column is expected to be a Struct of the type
        `struct<
            street1: string,
            street2: string,  # eg "Apt 3"
            city: string,
            state: string,
            postcode: string,  # zipcode in the US
            country: string,
        >`.

        Parameters
        ----------
        column_left:
            The column in the left table containing the name struct.
        column_right:
            The column in the right table containing the name struct.
        """
        self.column_left = column_left
        self.column_right = column_right
        self.name = name

    def __call__(self, t: it.Table) -> it.Table:
        al, ar = (
            _util.get_column(t, self.column_left),
            _util.get_column(t, self.column_right),
        )
        levels = [
            (
                "null",
                ibis.or_(
                    _util.struct_isnull(
                        al,
                        how="any",
                        fields=["street1", "city", "state"],
                    ),
                    _util.struct_isnull(
                        ar,
                        how="any",
                        fields=["street1", "city", "state"],
                    ),
                ),
            ),
            ("street1_and_city_or_postal_code", same_address_for_mailing(al, ar)),
            ("same_region", same_region(al, ar)),
            ("same_state", al.state == ar.state),
        ]
        return compare(t, LevelComparer(self.name, levels))


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
