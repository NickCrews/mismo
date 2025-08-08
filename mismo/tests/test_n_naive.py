from __future__ import annotations

import pytest

from mismo._n_naive import n_naive_comparisons


def test_dedupe_with_int():
    assert n_naive_comparisons(0) == 0
    assert n_naive_comparisons(1) == 0
    assert n_naive_comparisons(2) == 1
    assert n_naive_comparisons(3) == 3
    assert n_naive_comparisons(4) == 6
    assert n_naive_comparisons(5) == 10


def test_dedupe_with_sized_list():
    left = [1, 2, 3, 4]
    assert n_naive_comparisons(left) == 4 * 3 // 2


def test_dedupe_with_ibis_table(table_factory):
    t = table_factory({"x": [1, 2, 3]})
    # falls back to count().execute() when len() isn't available
    assert n_naive_comparisons(t) == 3 * 2 // 2


def test_link_with_ints():
    assert n_naive_comparisons(3, 4) == 12


def test_link_with_mixed_sized_and_int():
    left = [1, 2, 3]
    assert n_naive_comparisons(left, 4) == 12
    assert n_naive_comparisons(4, left) == 12


@pytest.mark.usefixtures("backend")
def test_link_with_ibis_tables(table_factory):
    left = table_factory({"x": [1, 2, 3]})
    right = table_factory({"y": [10, 20, 30, 40]})
    assert n_naive_comparisons(left, right) == 12
