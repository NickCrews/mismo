from __future__ import annotations

import pytest

from mismo.linker import IDLinker
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def a(table_factory):
    return table_factory(
        {
            "record_id": [1, 2, 3, 4],
            "label": [9, 9, None, 8],
        }
    )


@pytest.fixture
def b(table_factory):
    return table_factory(
        {
            "record_id": [11, 12, 13, 14],
            "label": [7, None, 8, 8],
        }
    )


@pytest.mark.xfail(raises=NotImplementedError)
@pytest.mark.parametrize(
    "when_not_equal_val, when_null_val, expected_data",
    [
        (
            "nonmatch",
            "nonmatch",
            [],
        ),
        (
            "nonmatch",
            "indefinite",
            [
                (3, 11),
                (3, 12),
                (3, 13),
                (3, 14),
                (1, 12),
                (2, 12),
                (4, 12),
            ],
        ),
        (
            "indefinite",
            "nonmatch",
            [
                (1, 11),
                (1, 13),
                (1, 14),
                (2, 11),
                (2, 13),
                (2, 14),
                (4, 11),
            ],
        ),
        (
            "indefinite",
            "indefinite",
            [
                (1, 11),
                (1, 12),
                (1, 13),
                (1, 14),
                (2, 11),
                (2, 12),
                (2, 13),
                (2, 14),
                (3, 11),
                (3, 12),
                (3, 13),
                (3, 14),
                (4, 11),
                (4, 12),
            ],
        ),
    ],
)
def test_indefinite_linkage(
    table_factory, a, b, when_not_equal_val, when_null_val, expected_data
):
    linker = IDLinker(
        "label", when_not_equal=when_not_equal_val, when_null=when_null_val
    )
    linkage = linker.indefinite_linkage(a, b)
    actual = linkage.links.select("record_id_l", "record_id_r")
    expected = table_factory(
        expected_data,
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual)


@pytest.mark.parametrize(
    "when_not_equal_val, when_null_val",
    [
        ("nonmatch", "nonmatch"),
        ("nonmatch", "indefinite"),
        ("indefinite", "nonmatch"),
        ("indefinite", "indefinite"),
    ],
)
def test_match_linkage(table_factory, a, b, when_not_equal_val, when_null_val):
    linker = IDLinker(
        "label", when_not_equal=when_not_equal_val, when_null=when_null_val
    )
    linkage = linker.match_linkage(a, b)
    actual = linkage.links.select("record_id_l", "record_id_r")
    expected = table_factory(
        [
            (4, 13),
            (4, 14),
        ],
        schema=actual.schema(),
    )
    assert_tables_equal(expected, actual)
