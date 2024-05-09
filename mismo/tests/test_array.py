from __future__ import annotations

import ibis
from ibis.expr import datatypes as dt
import pytest

from mismo import _array

mark_dtype = pytest.mark.parametrize("dtype", [dt.Int16(), dt.Float32()])


@mark_dtype
def test_array_min(backend, dtype):
    array = ibis.literal([1, 2, 3, None]).cast(dt.Array(value_type=dtype))
    result = _array.array_min(array)
    assert result.type() == dtype
    assert result.execute() == 1


@mark_dtype
def test_array_max(backend, dtype):
    array = ibis.literal([1, 2, 3, None]).cast(dt.Array(value_type=dtype))
    result = _array.array_max(array)
    assert result.type() == dtype
    assert result.execute() == 3
