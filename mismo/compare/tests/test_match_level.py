from __future__ import annotations

import ibis
import pytest

from mismo.compare import MatchLevels


def test_match_levels():
    class EmailMatchLevelss(MatchLevels):
        FULL_EXACT = 0
        FULL_NEAR = 1
        LOCAL_EXACT = 2
        LOCAL_NEAR = 3
        ELSE = 4
        _UNDERED = 4
        NOT_INT = "foo"

        def ignored(self):
            pass

    assert EmailMatchLevelss.FULL_EXACT.as_integer() == 0
    assert EmailMatchLevelss.FULL_EXACT.as_string() == "FULL_EXACT"
    assert str(EmailMatchLevelss.FULL_EXACT) == "FULL_EXACT"
    assert (
        str(EmailMatchLevelss)
        == "EmailMatchLevelss(FULL_EXACT=0, FULL_NEAR=1, LOCAL_EXACT=2, LOCAL_NEAR=3, ELSE=4)"  # noqa: E501
    )
    assert len(EmailMatchLevelss) == 5
    assert "FULL_EXACT" in EmailMatchLevelss
    assert 0 in EmailMatchLevelss
    assert -1 not in EmailMatchLevelss
    assert bool not in EmailMatchLevelss

    assert EmailMatchLevelss[0] == "FULL_EXACT"
    assert EmailMatchLevelss["FULL_EXACT"] == 0
    with pytest.raises(TypeError):
        EmailMatchLevelss[None]

    assert EmailMatchLevelss(1).as_integer() == 1
    assert EmailMatchLevelss(1).as_string() == "FULL_NEAR"
    assert EmailMatchLevelss("FULL_NEAR").as_integer() == 1
    assert EmailMatchLevelss("FULL_NEAR").as_string() == "FULL_NEAR"

    with pytest.raises(ValueError):
        EmailMatchLevelss("foo")
    with pytest.raises(ValueError):
        EmailMatchLevelss(5)

    ei = ibis.literal(1)
    es = ibis.literal("FULL_NEAR")
    assert EmailMatchLevelss(ei).as_integer().execute() == 1
    assert EmailMatchLevelss(ei).as_string().execute() == "FULL_NEAR"
    assert EmailMatchLevelss(es).as_integer().execute() == 1
    assert EmailMatchLevelss(es).as_string().execute() == "FULL_NEAR"
