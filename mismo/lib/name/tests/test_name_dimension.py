from __future__ import annotations

import ibis

from mismo.block import block_one
from mismo.lib.name import NameDimension


def test_name_dimension(name_table):
    dim = NameDimension("name")
    name_table = dim.prep(name_table)
    real_pairs = [
        (1, 2),
        (1, 3),
        (3, 4),
        (5, 6),
    ]
    blocked = block_one(
        name_table,
        name_table,
        lambda left, right, **_: ibis.array([left.record_id, right.record_id]).isin(
            real_pairs
        ),
    )
    compared = dim.compare(blocked)
    compared = compared.order_by(["record_id_l", "record_id_r"])
    assert compared.execute().NameDimension.to_list() == [
        "initials",
        "else",
        "nicknames",
        "first_last",
    ]
