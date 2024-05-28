from __future__ import annotations

import ibis
from ibis import _

from mismo.block import CrossBlocker
from mismo.lib.name import NameDimension, NameMatchLevels


def test_name_dimension(name_table):
    dim = NameDimension("name")
    name_table = dim.prep(name_table)
    real_pairs = [
        (1, 2),
        (1, 3),
        (3, 4),
        (5, 6),
    ]
    blocked = CrossBlocker()(name_table, name_table).filter(
        ibis.array([_.record_id_l, _.record_id_r]).isin(real_pairs)
    )
    compared = dim.compare(blocked)
    compared = compared.order_by(["record_id_l", "record_id_r"])
    assert compared.execute().name_compared.to_list() == [
        NameMatchLevels.INITIALS,
        NameMatchLevels.ELSE,
        NameMatchLevels.NICKNAMES,
        NameMatchLevels.FIRST_LAST,
    ]
