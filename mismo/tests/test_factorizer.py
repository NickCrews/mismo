from __future__ import annotations

import pytest

from mismo._factorizer import Factorizer
from mismo.tests.util import get_clusters


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
            id="float",
        ),
    ],
)
def test_factorizer(table_factory, values, values_type):
    data = {
        "id": range(len(values)),
        "values": [val for val, _code in values],
        "expected_codes": [code for _val, code in values],
    }
    schema = {
        "id": "int64",
        "values": values_type,
        "expected_codes": "int64",
    }
    t = table_factory(data).cast(schema)
    f = Factorizer(t, "values")

    e = f.encode()
    assert set(e.columns) == {"id", "values", "expected_codes"}
    assert_equal(e, "values", "expected_codes")

    e = f.encode(dst="encoded")
    assert set(e.columns) == {"id", "values", "expected_codes", "encoded"}
    assert_equal(e, "encoded", "expected_codes")

    restored = f.decode(e, src="encoded")
    assert set(restored.columns) == {"id", "values", "expected_codes", "encoded"}
    assert_equal(restored, "encoded", "values")

    restored = f.decode(e, src="encoded", dst="decoded")
    assert set(restored.columns) == {
        "id",
        "values",
        "expected_codes",
        "encoded",
        "decoded",
    }
    assert_equal(restored, "decoded", "values")

    t2 = t.mutate(values2=t.values)
    e = f.encode(t2)
    assert set(e.columns) == {"id", "values", "expected_codes", "values2"}
    assert_equal(e, "values", "expected_codes")

    e = f.encode(t2, src="values2", dst="codes2")
    assert set(e.columns) == {"id", "values", "expected_codes", "values2", "codes2"}
    assert_equal(e, "values", "values2")
    assert_equal(e, "codes2", "expected_codes")


def assert_equal(t, x, y):
    x_clusters = get_clusters(t[x], label=t.id)
    y_clusters = get_clusters(t[y], label=t.id)
    assert x_clusters == y_clusters
