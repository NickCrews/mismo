from __future__ import annotations

from ibis import _
import pytest

from mismo.block import KeyBlocker
from mismo.tests.util import assert_tables_equal


@pytest.fixture
def inp(table_factory):
    records = [
        ("a", 1),
        ("b", 1),
        ("b", 1),
        ("c", 3),
        ("b", 2),
        ("c", 3),
        (None, 4),
        ("c", 3),
    ]
    letters, nums = zip(*records)
    t = table_factory({"letter": letters, "num": nums})
    return t


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("letter", id="str"),
        pytest.param(_.letter, id="deferred"),
    ],
)
def test_key_counts_letters(inp, table_factory, key):
    blocker = KeyBlocker(key)
    expected = table_factory(
        {
            "letter": ["c", "b", "a"],
            "n": [3, 3, 1],
        }
    )
    result = blocker.key_counts(inp)
    assert_tables_equal(result, expected)

    expected_2 = expected.mutate(n=_.n * _.n)
    result2 = blocker.key_counts(inp, inp)
    assert_tables_equal(result2, expected_2)


@pytest.mark.parametrize(
    "key",
    [
        pytest.param(("letter", "num"), id="str"),
        pytest.param((_.letter, "num"), id="deferred"),
    ],
)
def test_key_counts_letters_num(inp, table_factory, key):
    blocker = KeyBlocker(key)
    expected = table_factory(
        {
            "letter": ["c", "b", "b", "a"],
            "num": [3, 1, 2, 1],
            "n": [3, 2, 1, 1],
        }
    )
    result = blocker.key_counts(inp)
    assert_tables_equal(result, expected)

    expected_2 = expected.mutate(n=_.n * _.n)
    result2 = blocker.key_counts(inp, inp)
    assert_tables_equal(result2, expected_2)


def test_key_counts_errs(inp):
    blocker = KeyBlocker("letter")
    # we only support positional args
    with pytest.raises(TypeError):
        blocker.key_counts(left=inp)
    with pytest.raises(TypeError):
        blocker.key_counts(left=inp, right=inp)
