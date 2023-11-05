from __future__ import annotations

import ibis
import numpy as np
import pytest

from mismo.compare._array import jaccard


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ([], [], 0),
        ([], [1], 0),
        ([1], [], 0),
        ([1], [1], 1),
        ([1], [1, 2], 1 / 2),
        (None, [1], np.nan),
        (None, None, np.nan),
        ([1, 2, 3, 4], [3, 4, 5, 6, 7, 8], 1 / 4),
    ],
)
def test_jaccard(a, b, expected):
    def make_array(x):
        if x is None:
            return ibis.literal(None).cast("array<int64>")
        else:
            return ibis.array(x)

    a = make_array(a)
    b = make_array(b)
    result = jaccard(a, b).execute()
    assert result == pytest.approx(expected, abs=0, nan_ok=True)
