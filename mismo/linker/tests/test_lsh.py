from __future__ import annotations

import ibis
import pytest

import mismo
from mismo.linker._lsh import p_blocked

sets = [
    # set(),
    {0},
    # {0, 1},
    {0, 1, 2},
    # {0, 1, 2, 3},
]


@pytest.mark.skip(reason="MinhashLshBlocker isn't implemented correctly")
@pytest.mark.parametrize("a", sets, ids=str)
@pytest.mark.parametrize("b", sets, ids=str)
@pytest.mark.parametrize("band_size", [5])
@pytest.mark.parametrize("n_bands", [10])
def test_minhash_lsh_blocker(table_factory, a, b, band_size: int, n_bands: int):
    left = table_factory({"terms": [a] * 100}, schema={"terms": "array<int>"})
    right = table_factory({"terms": [b] * 100}, schema={"terms": "array<int>"})
    left = left.mutate(record_id=ibis.row_number())
    right = right.mutate(record_id=ibis.row_number())
    blocker = mismo.linkage.MinhashLshBlocker(
        terms_column="terms", band_size=band_size, n_bands=n_bands
    )
    p_expected = p_blocked(_jaccard(a, b), band_size=band_size, n_bands=n_bands)
    n_expected = (100 * 100) * p_expected
    blocked = blocker(left, right).execute()
    assert len(blocked) == pytest.approx(n_expected, rel=0.1)


def _jaccard(a, b):
    if not a and not b:
        return 0
    return len(a & b) / len(a | b)
