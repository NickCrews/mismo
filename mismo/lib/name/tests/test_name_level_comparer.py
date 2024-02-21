from __future__ import annotations

import ibis

from mismo.block import block_many
from mismo.compare import compare
from mismo.lib.name import NameLevelComparer


def test_name_level_comparer(name_table):
    comparer = NameLevelComparer("name_l", "name_r", name="my_level")
    real_pairs = [
        (1, 2),
        (3, 4),
        (5, 6),
    ]
    blocked = block_many(
        name_table,
        name_table,
        lambda left, right, **_: ibis.array([left.record_id, right.record_id]).isin(
            real_pairs
        ),
    )
    compared = compare(blocked, comparer)
    compared = compared.order_by("record_id_l")
    assert compared.execute().my_level.to_list() == [
        "initials",
        "nicknames",
        "else",
    ]
