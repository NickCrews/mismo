from __future__ import annotations

import ibis
import pytest

from mismo.compare import MatchLevels


def test_match_levels():
    class EmailMatchLevels(MatchLevels):
        FULL_EXACT = 0
        FULL_NEAR = 1
        LOCAL_EXACT = 2
        LOCAL_NEAR = 3
        ELSE = 4
        _UNDERED = 4
        NOT_INT = "foo"

        def ignored(self):
            pass

    assert EmailMatchLevels.FULL_EXACT.as_integer() == 0
    assert EmailMatchLevels.FULL_EXACT.as_string() == "FULL_EXACT"
    assert str(EmailMatchLevels.FULL_EXACT) == "FULL_EXACT"
    assert (
        str(EmailMatchLevels)
        == "EmailMatchLevels(FULL_EXACT=0, FULL_NEAR=1, LOCAL_EXACT=2, LOCAL_NEAR=3, ELSE=4)"  # noqa: E501
    )
    assert len(EmailMatchLevels) == 5
    assert "FULL_EXACT" in EmailMatchLevels
    assert 0 in EmailMatchLevels
    assert -1 not in EmailMatchLevels
    assert bool not in EmailMatchLevels

    assert EmailMatchLevels[0] == "FULL_EXACT"
    assert EmailMatchLevels["FULL_EXACT"] == 0
    with pytest.raises(TypeError):
        EmailMatchLevels[None]

    assert EmailMatchLevels(1).as_integer() == 1
    assert EmailMatchLevels(1).as_string() == "FULL_NEAR"
    assert EmailMatchLevels("FULL_NEAR").as_integer() == 1
    assert EmailMatchLevels("FULL_NEAR").as_string() == "FULL_NEAR"

    with pytest.raises(ValueError):
        EmailMatchLevels("foo")
    with pytest.raises(ValueError):
        EmailMatchLevels(5)

    ei = ibis.literal(1)
    es = ibis.literal("FULL_NEAR")
    assert EmailMatchLevels(ei).as_integer().execute() == 1
    assert EmailMatchLevels(ei).as_string().execute() == "FULL_NEAR"
    assert EmailMatchLevels(es).as_integer().execute() == 1
    assert EmailMatchLevels(es).as_string().execute() == "FULL_NEAR"
