from __future__ import annotations

import ibis
import pytest

from mismo import sets
from mismo.tests.util import assert_tables_equal


def test_document_counts(table_factory):
    addresses = [
        "12 main st",
        "34 st john ave",
        "56 oak st",
        "12 glacier st",
    ]
    t = table_factory({"address": addresses})
    tokens = t.address.re_split(r"\s+")
    result = sets.document_counts(tokens)
    expected_records = [
        ("12", 2),
        ("56", 1),
        ("main", 1),
        ("34", 1),
        ("glacier", 1),
        ("oak", 1),
        ("john", 1),
        ("st", 4),
        ("ave", 1),
    ]
    expected = ibis.memtable(expected_records, columns=["term", "n_records"])
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
def test_term_counts(table_factory, terms, expected):
    typ = "string" if terms and isinstance(terms[0], str) else "int64"
    inp = table_factory({"terms": [terms]}, schema={"terms": f"array<{typ}>"})
    result = sets.add_array_value_counts(inp, "terms")
    expected = table_factory(
        {"terms": [terms], "terms_counts": [expected]},
        schema={"terms": f"array<{typ}>", "terms_counts": f"map<{typ}, int64>"},
    )
    assert_tables_equal(result, expected)


def test_add_tfidf(table_factory):
    terms = [
        None,
        [],
        ["st"],
        ["12", "main", "st"],
        ["99", "main", "ave"],
        ["56", "st", "joseph", "st"],
        ["21", "glacier", "st"],
        ["12", "glacier", "st"],
    ]
    t = table_factory({"terms": terms})
    result = sets.add_tfidf(t, "terms", normalize=False)
    expected_records = [
        {"terms": ["st"], "terms_tfidf": {"st": 0.3364722366212129}},
        {
            "terms": ["12", "main", "st"],
            "terms_tfidf": {
                "12": 1.252762968495368,
                "main": 1.252762968495368,
                "st": 0.3364722366212129,
            },
        },
        {
            "terms": ["99", "main", "ave"],
            "terms_tfidf": {
                "99": 1.9459101490,
                "ave": 1.945910149,
                "main": 1.25276296,
            },
        },
        {
            "terms": ["56", "st", "joseph", "st"],
            "terms_tfidf": {
                "joseph": 1.9459101490553132,
                "56": 1.9459101490553132,
                "st": 0.6729444732424258,
            },
        },
        {
            "terms": ["21", "glacier", "st"],
            "terms_tfidf": {
                "21": 1.9459101490553132,
                "glacier": 1.252762968495368,
                "st": 0.3364722366212129,
            },
        },
        {
            "terms": ["12", "glacier", "st"],
            "terms_tfidf": {
                "12": 1.252762968495368,
                "glacier": 1.252762968495368,
                "st": 0.3364722366212129,
            },
        },
        {"terms": None, "terms_tfidf": None},
        {"terms": [], "terms_tfidf": {}},
    ]
    expected = ibis.memtable(
        expected_records,
        schema={
            "terms": "array<string>",
            "terms_tfidf": "map<string, float64>",
        },
    )
    assert_tables_equal(result, expected)
