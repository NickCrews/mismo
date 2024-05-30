from __future__ import annotations

import ibis
import numpy as np
import pytest

from mismo.sets import jaccard


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
    a = ibis.literal(a, type="array<int64>")
    b = ibis.literal(b, type="array<int64>")
    result = jaccard(a, b).execute()
    assert result == pytest.approx(expected, abs=0, nan_ok=True)
