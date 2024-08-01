from __future__ import annotations

import string
import random

import ibis
from ibis import _
import ibis.expr.types as ir
import pytest

from mismo import text
from rapidfuzz.process import cpdist
from rapidfuzz import fuzz
import pyarrow as pa

@ibis.udf.scalar.pyarrow
def levenshtein_ratio(s1: str, s2: str) -> float:
    return cpdist(s1.to_numpy(), s2.to_numpy())

@ibis.udf.scalar.python
def ratio(s1: str, s2: str) -> float:
    return fuzz.ratio(s1, s2)

def create_test_data() -> ir.Table:
    random.seed(0)
    words = [
        "".join(random.choice(string.ascii_letters + string.digits) for _ in range(10))
        for _ in range(10_000)
    ]
    arr1 = random.choices(words, k=10_000_000)
    arr2 = random.choices(words, k=10_000_000)
    return ibis.memtable({"s1": arr1, "s2": arr2})

@pytest.fixture
def data(backend: ibis.BaseBackend) -> ir.Table:
     t = backend.create_table("data",create_test_data())
     t = t.cache()
     return t


@pytest.mark.parametrize(
    "fn",
    [
        pytest.param(ratio, id="rapidfuzz"),
        pytest.param(levenshtein_ratio, id="rapidfuzz-process"),
        pytest.param(text.levenshtein_ratio, id="mismo"),
    ],
)
@pytest.mark.parametrize(
    "nrows",
    [
        pytest.param(1_000, id="1k"),
        pytest.param(10_000, id="10k"),
        pytest.param(100_000, id="100k"),
        pytest.param(1_000_000, id="1m"),
        pytest.param(10_000_000, id="10m"),
    ],
)
def test_benchmark_similarity(backend: ibis.BaseBackend, data, nrows, fn, benchmark):
    inp = data.head(nrows).cache()

    def run():
        t = inp.mutate(result=fn(inp.s1, inp.s2))
        return backend.create_table("temp", t, overwrite=True)

    result = benchmark(run)
    assert len(result.execute()) == nrows
