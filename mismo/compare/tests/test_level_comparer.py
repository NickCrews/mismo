from __future__ import annotations

import ibis
import pytest

from mismo.arrays import array_any
from mismo.compare import LevelComparer, MatchLevel


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


def test_constructors():
    assert EmailMatchLevel(1).as_integer() is 1  # noqa: F632
    assert EmailMatchLevel("FULL_NEAR").as_integer() is 1  # noqa: F632
    with pytest.raises(TypeError):
        EmailMatchLevel(None)
    with pytest.raises(ValueError):
        EmailMatchLevel(100)
    with pytest.raises(ValueError):
        EmailMatchLevel("full_near")


def test_getitem():
    assert isinstance(EmailMatchLevel[1], str)
    assert EmailMatchLevel[1] == "FULL_NEAR"

    assert isinstance(EmailMatchLevel["FULL_NEAR"], int)
    assert EmailMatchLevel["FULL_NEAR"] == 1

    with pytest.raises(TypeError):
        EmailMatchLevel[None]
    with pytest.raises(KeyError):
        EmailMatchLevel[100]
    with pytest.raises(KeyError):
        EmailMatchLevel["full_near"]

    assert EmailMatchLevel[ibis.literal(1)].execute() == "FULL_NEAR"
    assert EmailMatchLevel[ibis.literal("FULL_NEAR")].execute() == 1

    assert EmailMatchLevel[ibis.literal(100)].execute() is None
    assert EmailMatchLevel[ibis.literal("full_near")].execute() is None


def test_container_semantics():
    assert len(EmailMatchLevel) == 5
    assert "FULL_EXACT" in EmailMatchLevel
    assert "full_exact" not in EmailMatchLevel
    assert 0 in EmailMatchLevel
    assert -1 not in EmailMatchLevel
    with pytest.raises(TypeError):
        assert bool not in EmailMatchLevel
    with pytest.raises(TypeError):
        ibis.literal(0) in EmailMatchLevel
    with pytest.raises(TypeError):
        ibis.literal("FULL_EXACT") in EmailMatchLevel


def test_eq_neq():
    assert EmailMatchLevel.FULL_EXACT == 0
    assert EmailMatchLevel.FULL_EXACT != 1
    assert EmailMatchLevel("FULL_EXACT") == 0
    assert EmailMatchLevel("FULL_EXACT") != 1
    assert EmailMatchLevel(0) == 0
    assert EmailMatchLevel(0) != 1

    assert EmailMatchLevel.FULL_EXACT == "FULL_EXACT"
    assert EmailMatchLevel.FULL_EXACT != "full_exact"
    assert EmailMatchLevel("FULL_EXACT") == "FULL_EXACT"
    assert EmailMatchLevel("FULL_EXACT") != "full_exact"
    assert EmailMatchLevel(0) == "FULL_EXACT"
    assert EmailMatchLevel(0) != "full_exact"

    assert EmailMatchLevel.FULL_EXACT == EmailMatchLevel.FULL_EXACT
    assert EmailMatchLevel.FULL_EXACT != EmailMatchLevel.FULL_NEAR
    assert EmailMatchLevel("FULL_EXACT") == EmailMatchLevel.FULL_EXACT
    assert EmailMatchLevel("FULL_EXACT") != EmailMatchLevel.FULL_NEAR
    assert EmailMatchLevel(0) == EmailMatchLevel.FULL_EXACT
    assert EmailMatchLevel(0) != EmailMatchLevel.FULL_NEAR


def test_ordering_not_supported():
    with pytest.raises(TypeError):
        EmailMatchLevel.FULL_EXACT < 1
    with pytest.raises(TypeError):
        EmailMatchLevel.FULL_EXACT <= 1
    with pytest.raises(TypeError):
        EmailMatchLevel.FULL_EXACT > 1
    with pytest.raises(TypeError):
        EmailMatchLevel.FULL_EXACT >= 1

    with pytest.raises(TypeError):
        EmailMatchLevel("FULL_EXACT") < 1
    with pytest.raises(TypeError):
        EmailMatchLevel("FULL_EXACT") <= 1
    with pytest.raises(TypeError):
        EmailMatchLevel("FULL_EXACT") > 1
    with pytest.raises(TypeError):
        EmailMatchLevel("FULL_EXACT") >= 1


def test_repr():
    assert (
        repr(EmailMatchLevel)
        == "EmailMatchLevel(FULL_EXACT=0, FULL_NEAR=1, LOCAL_EXACT=2, LOCAL_NEAR=3, ELSE=4)"  # noqa: E501
    )
    assert repr(EmailMatchLevel.FULL_EXACT) == "EmailMatchLevel.FULL_EXACT"
    assert repr(EmailMatchLevel("FULL_EXACT")) == "EmailMatchLevel.FULL_EXACT"
    assert repr(EmailMatchLevel(0)) == "EmailMatchLevel.FULL_EXACT"

    ibis_repr = "EmailMatchLevel(<foo>)"
    assert repr(EmailMatchLevel(ibis.literal("FULL_EXACT").name("foo"))) == ibis_repr
    assert repr(EmailMatchLevel(ibis.literal(0).name("foo"))) == ibis_repr


def test_str():
    # class
    assert (
        str(EmailMatchLevel)
        == "EmailMatchLevel(FULL_EXACT=0, FULL_NEAR=1, LOCAL_EXACT=2, LOCAL_NEAR=3, ELSE=4)"  # noqa: E501
    )
    # instance
    assert str(EmailMatchLevel.FULL_EXACT) == "FULL_EXACT"
    assert str(EmailMatchLevel("FULL_EXACT")) == "FULL_EXACT"
    assert str(EmailMatchLevel(0)) == "FULL_EXACT"
    with pytest.raises(TypeError):
        str(EmailMatchLevel(ibis.literal("FULL_EXACT")))
    with pytest.raises(TypeError):
        str(EmailMatchLevel(ibis.literal(0)))


def test_int():
    # class
    with pytest.raises(TypeError):
        int(EmailMatchLevel)
    # instance
    assert int(EmailMatchLevel.FULL_EXACT) == 0
    assert int(EmailMatchLevel("FULL_EXACT")) == 0
    assert int(EmailMatchLevel(0)) == 0
    with pytest.raises(TypeError):
        int(EmailMatchLevel(ibis.literal("FULL_EXACT")))
    with pytest.raises(TypeError):
        int(EmailMatchLevel(ibis.literal(0)))


def test_conversion():
    assert EmailMatchLevel.FULL_NEAR.as_integer() == 1
    assert EmailMatchLevel.FULL_NEAR.as_string() == "FULL_NEAR"

    assert EmailMatchLevel(1).as_integer() == 1
    assert EmailMatchLevel(1).as_string() == "FULL_NEAR"
    assert EmailMatchLevel("FULL_NEAR").as_integer() == 1
    assert EmailMatchLevel("FULL_NEAR").as_string() == "FULL_NEAR"

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
