from __future__ import annotations

import ibis
import pytest

from mismo.arrays import array_any
from mismo.compare import LevelComparer, MatchLevel


def test_match_levels():
    class EmailMatchLevel(MatchLevel):
        FULL_EXACT = 0
        FULL_NEAR = 1
        LOCAL_EXACT = 2
        LOCAL_NEAR = 3
        ELSE = 4
        _UNDERED = 4
        NOT_INT = "foo"

        def ignored(self):
            pass

    assert EmailMatchLevel.FULL_EXACT.as_integer() == 0
    assert EmailMatchLevel.FULL_EXACT.as_string() == "FULL_EXACT"
    assert str(EmailMatchLevel.FULL_EXACT) == "FULL_EXACT"
    assert (
        str(EmailMatchLevel)
        == "EmailMatchLevel(FULL_EXACT=0, FULL_NEAR=1, LOCAL_EXACT=2, LOCAL_NEAR=3, ELSE=4)"  # noqa: E501
    )
    assert len(EmailMatchLevel) == 5
    assert "FULL_EXACT" in EmailMatchLevel
    assert 0 in EmailMatchLevel
    assert -1 not in EmailMatchLevel
    assert bool not in EmailMatchLevel

    assert EmailMatchLevel[0] == "FULL_EXACT"
    assert EmailMatchLevel["FULL_EXACT"] == 0
    with pytest.raises(TypeError):
        EmailMatchLevel[None]

    assert EmailMatchLevel(1) == 1
    assert EmailMatchLevel("FULL_NEAR") == 1
    assert EmailMatchLevel(1) == "FULL_NEAR"
    assert EmailMatchLevel("FULL_NEAR") == "FULL_NEAR"
    assert EmailMatchLevel(1).as_integer() == 1
    assert EmailMatchLevel(1).as_string() == "FULL_NEAR"
    assert EmailMatchLevel("FULL_NEAR").as_integer() == 1
    assert EmailMatchLevel("FULL_NEAR").as_string() == "FULL_NEAR"

    with pytest.raises(ValueError):
        EmailMatchLevel("foo")
    with pytest.raises(ValueError):
        EmailMatchLevel(5)

    ei = ibis.literal(1)
    es = ibis.literal("FULL_NEAR")
    assert EmailMatchLevel(ei).as_integer().execute() == 1
    assert EmailMatchLevel(ei).as_string().execute() == "FULL_NEAR"
    assert EmailMatchLevel(es).as_integer().execute() == 1
    assert EmailMatchLevel(es).as_string().execute() == "FULL_NEAR"


def test_match_levels_bad():
    with pytest.raises(TypeError):

        class DuplicateValues(MatchLevel):
            X = 1
            Y = 1


def test_array_based_conditions(table_factory):
    class TagMatchLevel(MatchLevel):
        ANY_EQUAL = 0
        ELSE = 1

    tag_comparer = LevelComparer(
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
