from __future__ import annotations

from ibis_enum import IbisEnum

from mismo.arrays import array_any
from mismo.compare import EnumComparer


def test_array_based_conditions(table_factory):
    class TagMatchLevel(IbisEnum):
        ANY_EQUAL = 0
        ELSE = 1

    tag_comparer = EnumComparer(
        name="tag",
        levels=TagMatchLevel,
        cases=[
            (
                lambda pairs: array_any(
                    pairs.tags_l.map(
                        lambda ltag: pairs.tags_r.map(lambda rtag: ltag == rtag)
                    ).flatten(),
                ),
                TagMatchLevel.ANY_EQUAL,
            ),
            (True, TagMatchLevel.ELSE),
        ],
        representation="string",
    )

    t = table_factory(
        {
            "tags_l": [["a", "b"], ["c", "d"], [], None],
            "tags_r": [["b", "x"], ["y", "z"], ["m"], []],
            "expected": ["ANY_EQUAL", "ELSE", "ELSE", "ELSE"],
        }
    )
    t = tag_comparer(t)
    assert t.tag.execute().tolist() == t.expected.execute().tolist()
