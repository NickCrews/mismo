from __future__ import annotations

import ibis
import pytest

from mismo.lib.email import EmailMatchLevel, match_level, parse_and_normalize_email


@pytest.mark.parametrize(
    "input, expfull, expuser, expdomain",
    [
        pytest.param(
            " Bob.Jones@gmail.com",
            "bobjones@gmailcom",
            "bobjones",
            "gmailcom",
            id="bobjones",
        ),
        pytest.param("", None, None, None, id="empty"),
        pytest.param("  ", None, None, None, id="whitespace"),
        pytest.param("@", None, None, None, id="at"),
        pytest.param(" @  ", None, None, None, id="empty_at_empty"),
        pytest.param("anne@", "anne@", "anne", None, id="no_domain"),
        pytest.param(None, None, None, None, id="null"),
    ],
)
def test_parse_and_normalize_email(input, expfull, expuser, expdomain):
    result = parse_and_normalize_email(ibis.literal(input, type="string"))
    assert result.full.execute() == expfull
    assert result.user.execute() == expuser
    assert result.domain.execute() == expdomain


@pytest.mark.parametrize(
    "a, b, level_str",
    [
        pytest.param(
            " Bob.Jones@gmail.com", "bobjones@gmail.com", "FULL_EXACT", id="full_exact"
        ),
        pytest.param(
            " Bob.Janes@gmail.com", "bobjones@gmail.com", "FULL_NEAR", id="full_near"
        ),
        pytest.param(
            " Rob.Janes@gmail.com", "bobjones@gmail.com", "ELSE", id="two_different"
        ),
        pytest.param(
            " Bob.Jones@gmail.com", "bobjones@yahoo.com", "USER_EXACT", id="user_exact"
        ),
        pytest.param(
            " Bob.Janes@gmail.com", "bobjones@yahoo.com", "USER_NEAR", id="user_near"
        ),
    ],
)
def test_match_level(a, b, level_str):
    ml = match_level(ibis.literal(a), ibis.literal(b))
    assert ml.as_string().execute() == level_str
    assert ml.as_integer().execute() == EmailMatchLevel[level_str]
