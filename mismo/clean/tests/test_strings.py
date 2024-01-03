from __future__ import annotations

from ibis.expr.types import StringColumn
import pytest

from mismo.clean import _strings


@pytest.fixture
def string_column(column_factory) -> StringColumn:
    return column_factory(
        [
            "jane's   house",
            "Ross' house  ",
            "a",
            "",
            None,
            "bees\tall cook",
        ]
    )


def test_norm_whitespace(string_column):
    result = _strings.norm_whitespace(string_column)
    expected = ["jane's house", "Ross' house", "a", "", None, "bees all cook"]
    assert result.execute().tolist() == expected
