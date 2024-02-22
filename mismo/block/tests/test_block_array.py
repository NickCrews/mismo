from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as it
import pandas as pd
import pytest

from mismo.block import block_one, join_on_array
from mismo.tests.util import assert_tables_equal


@pytest.mark.parametrize(
    "left,right",
    [
        ("array", _.array),
        ("array", lambda t: t.array),
    ],
)
def test_array_blocker(table_factory, t1: it.Table, t2: it.Table, left, right):
    def f(tl, tr, **kwargs):
        return join_on_array(tl, tr, left, right, **kwargs)

    blocked = block_one(t1, t2, f)
    blocked_ids = blocked[["record_id_l", "record_id_r"]]
    expected = table_factory(
        pd.DataFrame(
            [(0, 90), (1, 90)],
            columns=["record_id_l", "record_id_r"],
        )
    )
    assert_tables_equal(expected, blocked_ids)


def _n_pairs(n: int, k: int) -> int:
    return (k - 1) * (n - k) + sum(range(k))


def _id_func(nk):
    n, k = nk
    return f"n={n},k={k},npairs={_n_pairs(n, k):,}"


@pytest.mark.parametrize(
    "nk",
    [
        (100_000, 5),
        (1_000_000, 5),
    ],
    ids=_id_func,
)
def test_benchmark_array_blocker(backend, benchmark, nk):
    n, k = nk
    t = ibis.memtable(
        {
            "record_id": list(range(n)),
            "vals": [list(range(i, i + k)) for i in range(n)],
        }
    )
    t = backend.create_table("t", t)
    i = 0

    def f():
        b = block_one(
            t,
            t,
            lambda left, right, **kwargs: join_on_array(
                left, right, "vals", "vals", **kwargs
            ),
        )
        # do this to prevent caching
        nonlocal i
        b = backend.create_table(f"b{i}", b)
        i += 1
        return b

    result = benchmark(f)
    result = result.order_by(["record_id_l", "record_id_r"])
    first_few_expected = [
        (0, 1),
        (0, 2),
        (0, 3),
        (0, 4),
        (1, 2),
        (1, 3),
        (1, 4),
        (1, 5),
        (2, 3),
        (2, 4),
    ]
    first_few = list(
        result[["record_id_l", "record_id_r"]]
        .head(len(first_few_expected))
        .to_pandas()
        .itertuples(index=False, name=None)
    )
    assert first_few_expected == list(first_few)
    n_expected = _n_pairs(n, k)
    assert n_expected == result.count().execute()
