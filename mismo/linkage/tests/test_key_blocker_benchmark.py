from __future__ import annotations

import ibis
import pytest

import mismo


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
def test_benchmark_block_array(backend, benchmark, nk):
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
        b = mismo.linker.UnnestLinker("vals")(t, t)
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
