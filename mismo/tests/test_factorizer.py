from __future__ import annotations

import pandas._testing as tm
import pytest

from mismo._factorizer import Factorizer


@pytest.mark.parametrize(
    "values, values_type",
    [
        pytest.param(
            [
                ("a", 0),
                ("a", 0),
                ("b", 1),
                ("b", 1),
                ("c", 2),
                (None, None),
            ],
            "string",
            id="simple",
        ),
        pytest.param(
            [],
            "string",
            id="empty_string",
        ),
        pytest.param(
            [],
            "int",
            id="empty_int",
        ),
        pytest.param(
            [(4, 4), (3, 3), (2, 2), (None, None)],
            "int64",
            id="int64",
        ),
        pytest.param(
            [(4, 2), (3, 1), (2, 0), (None, None)],
            "uint64",
            id="uint64",
        ),
        pytest.param(
            [
                (1.0, 0),
                (3.5, 1),
                (3.5, 1),
                (None, None),
            ],
            "float",
            id="floats",
        ),
    ],
)
def test_factorizer(table_factory, values, values_type):
    data = {
        "values": [val for val, _code in values],
        "expected_codes": [code for _val, code in values],
    }
    schema = {
        "values": values_type,
        "expected_codes": "int64",
    }
    t = table_factory(data).cast(schema)
    f = Factorizer(t, "values")

    e = f.encode()
    assert set(e.columns) == {"values", "expected_codes"}
    assert_equal(e.values, e.expected_codes)

    e = f.encode(dst="encoded")
    assert set(e.columns) == {"values", "expected_codes", "encoded"}
    assert_equal(e.encoded, e.expected_codes)

    restored = f.decode(e, src="encoded")
    assert set(restored.columns) == {"values", "expected_codes", "encoded"}
    assert_equal(restored.encoded, restored.values)

    restored = f.decode(e, src="encoded", dst="decoded")
    assert set(restored.columns) == {"values", "expected_codes", "encoded", "decoded"}
    assert_equal(restored.decoded, restored.values)

    t2 = t.mutate(values2=t.values)
    e = f.encode(t2)
    assert set(e.columns) == {"values", "expected_codes", "values2"}
    assert_equal(e.values, e.expected_codes)

    e = f.encode(t2, src="values2", dst="codes2")
    assert set(e.columns) == {"values", "expected_codes", "values2", "codes2"}
    assert_equal(e.values, e.values2)
    assert_equal(e.codes2, e.expected_codes)


def assert_equal(x, y):
    x = x.to_pandas()
    y = y.to_pandas()
    tm.assert_series_equal(x, y, check_names=False, check_dtype=False)
