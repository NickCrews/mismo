from __future__ import annotations

import ibis
from ibis.expr.types import StringColumn, Table
import pandas as pd
import pytest

from mismo.clean import strings


@pytest.fixture
def string_table() -> Table:
    data = ["jane's   house", "Ross' house  ", "a", "", None, "bees\tall cook"]
    df = pd.DataFrame({"strings": data})
    return ibis.memtable(df)


@pytest.fixture
def string_column(string_table: Table) -> StringColumn:
    return string_table["strings"]  # type: ignore


def test_norm_whitespace(string_column):
    result = strings.norm_whitespace(string_column)
    expected = ["jane's house", "Ross' house", "a", "", None, "bees all cook"]
    assert result.execute().tolist() == expected


def test_norm_possessives(string_column):
    result = strings.norm_possessives(string_column)
    expected = ["janes   house", "Ross' house  ", "a", "", None, "bees\tall cook"]
    assert result.execute().tolist() == expected
