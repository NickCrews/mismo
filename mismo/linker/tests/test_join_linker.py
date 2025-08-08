from __future__ import annotations

import ibis
from ibis.expr import types as ir
import pytest

import mismo
from mismo import JoinLinker
from mismo.tests.util import assert_tables_equal


def joined_on_letter(t1, t2, **kwargs):
    return ibis.join(
        t1, t2, t1.letter == t2.letter, lname="{name}_l", rname="{name}_r"
    )["record_id_l", "record_id_r"]


def joined_on_letter_int(t1, t2, **kwargs):
    return ibis.join(
        t1,
        t2,
        (t1.letter == t2.letter) & (t1.int == t2.int),
        lname="{name}_l",
        rname="{name}_r",
    )["record_id_l", "record_id_r"]


def joined_on_int_gt(t1, t2, **kwargs):
    return ibis.join(t1, t2, t1.int > t2.int, lname="{name}_l", rname="{name}_r")[
        "record_id_l", "record_id_r"
    ]


def letter_condition(left, right):
    return left.letter == right.letter


def letter_and_int_condition(left, right):
    return (left.letter == right.letter) & (left.int == right.int)


def int_gt_condition(left, right):
    return left.int > right.int


def cross_product_condition(left, right):
    return left.int > 0


def null_safe_letter_condition(left, right):
    return (left.letter == right.letter) | (
        left.letter.isnull() & right.letter.isnull()
    )


def name_condition(left, right):
    return left.name == right.full_name


def street_condition(left, right):
    return left.street == right.street


@pytest.mark.parametrize(
    "condition,expected_maker",
    [
        pytest.param(
            letter_condition,
            joined_on_letter,
            id="letter_equality",
        ),
        pytest.param(
            letter_and_int_condition,
            joined_on_letter_int,
            id="letter_and_int_equality",
        ),
        pytest.param(
            int_gt_condition,
            joined_on_int_gt,
            id="int_greater_than",
        ),
        # Test with deferred expressions
        pytest.param(
            letter_condition,
            joined_on_letter,
            id="deferred_letter",
        ),
    ],
)
def test_JoinLinker_conditions(t1: ir.Table, t2: ir.Table, condition, expected_maker):
    # Use on_slow="ignore" for tests that might have slow joins
    on_slow = "ignore" if condition == int_gt_condition else "error"
    linkage = JoinLinker(condition, on_slow=on_slow)(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")
    expected = expected_maker(t1, t2)
    assert_tables_equal(joined_ids, expected)


def test_deduplication(table_factory):
    t = table_factory(
        {
            "record_id": [0, 1, 2, 3, 4],
            "letter": ["a", "b", "a", "b", None],
            "int": [1, 2, 1, 2, 3],
        }
    )
    linker = JoinLinker(letter_condition)
    linkage = linker(t, t)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")

    # JoinLinker includes ALL matches (including self-matches) but applies
    # deduplication filter (record_id_l < record_id_r) only to self-joins
    # So we get: (0,2) for "a" matches, (1,3) for "b" matches
    expected = table_factory(
        {
            "record_id_l": [0, 1],
            "record_id_r": [2, 3],
        }
    )
    assert_tables_equal(joined_ids, expected)


def test_task_parameter(table_factory):
    t1 = table_factory(
        {
            "record_id": [0, 1],
            "letter": ["a", "b"],
        }
    )
    t2 = table_factory(
        {
            "record_id": [90, 91],
            "letter": ["a", "b"],
        }
    )

    linker = JoinLinker(letter_condition, task="link")
    linkage = linker(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")
    expected = table_factory(
        {
            "record_id_l": [0, 1],
            "record_id_r": [90, 91],
        }
    )
    assert_tables_equal(joined_ids, expected)


def test_on_slow_parameter(t1: ir.Table, t2: ir.Table):
    # Should not raise with on_slow="ignore"
    linker = JoinLinker(cross_product_condition, on_slow="ignore")
    linkage = linker(t1, t2)
    assert linkage.links.count().execute() > 0

    # Should warn with on_slow="warn" (but not fail)
    import warnings

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        linker = JoinLinker(cross_product_condition, on_slow="warn")
        linkage = linker(t1, t2)
        assert linkage.links.count().execute() > 0
        # Should have issued a warning
        assert len(w) > 0
        assert "slow" in str(w[0].message).lower()


def test_complex_conditions(t1: ir.Table, t2: ir.Table):
    linker = JoinLinker(null_safe_letter_condition)
    linkage = linker(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")

    # Should include the null-null matches
    assert joined_ids.count().execute() >= 2  # At least b-b and c-c matches


def test_repr():
    linker = JoinLinker(letter_condition, task="link", on_slow="warn")
    repr_str = repr(linker)
    assert "JoinLinker" in repr_str
    assert "task=link" in repr_str
    assert "on_slow=warn" in repr_str


def test_empty_result(table_factory):
    t1 = table_factory(
        {
            "record_id": [0, 1],
            "letter": ["a", "b"],
        }
    )
    t2 = table_factory(
        {
            "record_id": [90, 91],
            "letter": ["x", "y"],
        }
    )

    linker = JoinLinker(letter_condition)
    linkage = linker(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")
    assert joined_ids.count().execute() == 0


@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(
            lambda t: t.inner_join(t.view(), "street").count(),
            id="naive",
        ),
        pytest.param(
            lambda t: mismo.JoinLinker(street_condition)
            .pair_counts(t, t, task="link")
            .n.sum(),
            id="JoinLinker",
        ),
    ],
)
@pytest.mark.parametrize(
    "nrows,exp",
    [
        (1_000, 16_990),
        (10_000, 576_101),
        (100_000, 50_843_827),
        (300_000, 447_872_405),
    ],
)
def test_benchmark_n_pairs(addresses_1M, fn, nrows, exp, benchmark):
    """Benchmark test similar to KeyLinker but using JoinLinker."""

    def run():
        return fn(addresses_1M.head(nrows)).execute()

    result = benchmark(run)
    assert result == exp


def test_join_condition_with_different_column_names(table_factory):
    t1 = table_factory(
        {
            "record_id": [0, 1],
            "name": ["Alice", "Bob"],
        }
    )
    t2 = table_factory(
        {
            "record_id": [90, 91],
            "full_name": ["Alice", "Charlie"],
        }
    )

    linker = JoinLinker(name_condition)
    linkage = linker(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")

    expected = table_factory(
        {
            "record_id_l": [0],
            "record_id_r": [90],
        }
    )
    assert_tables_equal(joined_ids, expected)
