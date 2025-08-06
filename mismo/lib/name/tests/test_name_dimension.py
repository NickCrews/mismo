from __future__ import annotations

import ibis
from ibis import _

import mismo
from mismo.lib.name import NameDimension, NameMatchLevel


def test_name_dimension(name_table):
    dim = NameDimension("name")
    name_table = dim.prepare_for_blocking(name_table)
    name_table = dim.prepare_for_fast_linking(name_table)
    real_pairs = [
        (1, 2),
        (1, 3),
        (3, 4),
        (5, 6),
    ]
    links = mismo.full_linkage(name_table, name_table).links.filter(
        ibis.array([_.record_id_l, _.record_id_r]).isin(real_pairs)
    )
    compared = dim.compare(links)
    compared = compared.order_by(["record_id_l", "record_id_r"])
    assert compared.execute().name_compared.to_list() == [
        NameMatchLevel.INITIALS,
        NameMatchLevel.ELSE,
        NameMatchLevel.NICKNAMES,
        NameMatchLevel.GIVEN_SURNAME,
    ]
