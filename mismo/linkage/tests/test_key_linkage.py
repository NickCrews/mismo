from __future__ import annotations

import pytest

import mismo


@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(
            lambda t: t.inner_join(t.view(), "street").count(),
            id="naive",
        ),
        pytest.param(
            lambda t: mismo.KeyLinker("street").pair_counts(t, t, task="link").n.sum(),
            id="KeyLinker",
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
    """
    ---------------------------------------------------------------------------------------- benchmark 'test_benchmark_count_pairs': 8 tests ----------------------------------------------------------------------------------------
    Name (time in ms)                                               Min                 Max                Mean             StdDev              Median                IQR            Outliers       OPS            Rounds  Iterations
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    test_benchmark_count_pairs[1000-16990-KeyLinker]            12.2805 (2.39)      14.1857 (2.11)      12.9836 (2.31)      0.3989 (1.19)      12.9002 (2.33)      0.5088 (1.71)         15;2   77.0202 (0.43)         67           1
    test_benchmark_count_pairs[1000-16990-naive]                 5.1490 (1.0)        6.7088 (1.0)        5.6203 (1.0)       0.4985 (1.49)       5.5324 (1.0)       0.7077 (2.37)          2;0  177.9269 (1.0)          11           1
    test_benchmark_count_pairs[10000-576101-KeyLinker]          14.0206 (2.72)      16.7933 (2.50)      15.0547 (2.68)      0.4714 (1.41)      14.9923 (2.71)      0.6295 (2.11)         15;1   66.4246 (0.37)         61           1
    test_benchmark_count_pairs[10000-576101-naive]               8.0191 (1.56)      10.4203 (1.55)       8.7055 (1.55)      0.3354 (1.0)        8.6626 (1.57)      0.2980 (1.0)          24;8  114.8695 (0.65)        101           1
    test_benchmark_count_pairs[100000-50843827-KeyLinker]       31.2830 (6.08)      34.3007 (5.11)      32.7514 (5.83)      0.6897 (2.06)      32.7460 (5.92)      0.6736 (2.26)         10;3   30.5330 (0.17)         31           1
    test_benchmark_count_pairs[100000-50843827-naive]          171.5182 (33.31)    173.2984 (25.83)    172.2893 (30.65)     0.5982 (1.78)     172.2069 (31.13)     0.5607 (1.88)          2;0    5.8042 (0.03)          6           1
    test_benchmark_count_pairs[300000-447872405-KeyLinker]      45.9068 (8.92)      57.3661 (8.55)      52.2799 (9.30)      2.8730 (8.57)      52.7966 (9.54)      3.9854 (13.37)         5;0   19.1278 (0.11)         20           1
    test_benchmark_count_pairs[300000-447872405-naive]         601.2822 (116.78)   652.4741 (97.26)    621.1668 (110.52)   20.6676 (61.63)    612.4148 (110.70)   29.6653 (99.54)         1;0    1.6099 (0.01)          5           1
    ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    It looks like the naive way starts off only 2x slower for small tables,
    but becomes more than 10x slower for large tables,
    and presumably at much larger tables simply becomes infeasible.
    """  # noqa: E501

    def run():
        return fn(addresses_1M.head(nrows)).execute()

    result = benchmark(run)
    assert result == exp
