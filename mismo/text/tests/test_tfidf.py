from __future__ import annotations

import ibis
import pytest

from mismo import text
from mismo.tests.util import assert_tables_equal


def test_document_frequency_table(table_factory):
    addresses = [
        "12 main st",
        "34 st john ave",
        "56 oak st",
        "12 glacier st",
    ]
    t = table_factory({"address": addresses})
    tokens = t.address.re_split(r"\s+")
    result = text.document_frequency_table(tokens)
    expected_records = [
        ("12", 2, 0.5, 0.6931471805599453),
        ("56", 1, 0.25, 1.3862943611198906),
        ("main", 1, 0.25, 1.3862943611198906),
        ("34", 1, 0.25, 1.3862943611198906),
        ("glacier", 1, 0.25, 1.3862943611198906),
        ("oak", 1, 0.25, 1.3862943611198906),
        ("john", 1, 0.25, 1.3862943611198906),
        ("st", 4, 1.0, 0.0),
        ("ave", 1, 0.25, 1.3862943611198906),
    ]
    expected = ibis.memtable(
        expected_records, columns=["term", "n_records", "frac_records", "idf"]
    )
    assert_tables_equal(result, expected)


@pytest.mark.parametrize(
    "terms, expected",
    [
        pytest.param(
            ["a", "b", "a", "b", "c", None],
            {
                "a": 2,
                "b": 2,
                "c": 1,
            },
            id="string",
        ),
        pytest.param(
            [5, 3, 3, None],
            {
                5: 1,
                3: 2,
            },
            id="int",
        ),
        pytest.param(
            [],
            {},
            id="empty",
        ),
        pytest.param(
            None,
            None,
            id="null",
        ),
    ],
)
def test_term_counts(terms, expected):
    typ = "array<string>" if terms and isinstance(terms[0], str) else "array<int64>"
    result = text.term_counts(ibis.literal(terms, type=typ)).execute()
    assert result == expected
