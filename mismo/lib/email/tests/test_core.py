from __future__ import annotations

import ibis
import pytest

from mismo.lib import email


@pytest.mark.parametrize(
    "input, exp",
    [
        pytest.param(
            " Bob.Jones@gmail.com",
            "bob.jones@gmail.com",
            id="bobjones",
        ),
        pytest.param("", None, id="empty"),
        pytest.param("  ", None, id="whitespace"),
        pytest.param(None, None, id="null"),
        pytest.param("@", None, id="at"),
        pytest.param(" @  ", None, id="empty_at_empty"),
        pytest.param("anne@", None, id="no_domain"),
        pytest.param("@gmail.com", None, id="no_user"),
        pytest.param("A@B ", "a@b", id="short"),
    ],
)
def test_clean_email(input, exp):
    result = email.clean_email(ibis.literal(input, type="string")).execute()
    assert result == exp


@pytest.mark.parametrize(
    "input, expfull, expuser, expdomain",
    [
        pytest.param(
            "bob.jones@gmail.com",
            "bob.jones@gmail.com",
            "bob.jones",
            "gmail.com",
            id="bobjones",
        ),
        pytest.param("", "", None, None, id="empty"),
        pytest.param(None, None, None, None, id="null"),
    ],
)
def test_split_email_address(input, expfull, expuser, expdomain):
    result = email.split_email(ibis.literal(input, str)).execute()
    assert result["full"] == expfull
    assert result["user"] == expuser
    assert result["domain"] == expdomain


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
    ml = email.match_level(ibis.literal(a), ibis.literal(b))
    assert ml.as_string().execute() == level_str
    assert ml.as_integer().execute() == email.EmailMatchLevel[level_str]
