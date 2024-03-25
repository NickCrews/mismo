from __future__ import annotations

import ibis

from mismo.block import block_one
from mismo.compare import compare
from mismo.lib.name import NameDimension


def test_name_dimension(name_table):
    comparer = NameDimension("ignore", column_normed="name")
    real_pairs = [
        (1, 2),
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
    compared = compare(blocked, comparer.prep)
    compared = compared.order_by("record_id_l")
    assert compared.execute().my_level.to_list() == [
        "initials",
        "nicknames",
        "else",
    ]
