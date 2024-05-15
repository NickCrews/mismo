from __future__ import annotations

from typing import Callable, Iterable

import ibis
from ibis.expr import types as ir
import pytest

# we want to have pytest assert introspection in the helpers
pytest.register_assert_rewrite("mismo.tests.util")


@pytest.fixture
def backend() -> ibis.BaseBackend:
    return ibis.duckdb.connect()


_count = 0


@pytest.fixture
def table_factory(backend: ibis.BaseBackend) -> Callable[..., ir.Table]:
    def factory(data, schema=None, columns=None, **kwargs):
        global _count
        name = f"__mismo_test{_count}"
        _count += 1
        mt = ibis.memtable(data, schema=schema, columns=columns)
        result = backend.create_table(name, mt, **kwargs)
        return result

    return factory


@pytest.fixture
def column_factory(table_factory) -> Callable[[Iterable], ir.Column]:
    def func(column_data):
        table = table_factory({"column": column_data})
        return table.column

    return func


@pytest.fixture
def t1(table_factory) -> ir.Table:
    return table_factory(
        {
            "record_id": [0, 1, 2],
            "int": [1, 2, 3],
            "letter": ["a", "b", "c"],
            "array": [["a", "b"], ["b"], []],
        }
    )


@pytest.fixture
def t2(table_factory) -> ir.Table:
    return table_factory(
        {
            "record_id": [90, 91, 92, 93],
            "int": [2, 4, None, 6],
            "letter": ["b", "c", "d", None],
            "array": [["b"], ["c"], ["d"], None],
        }
    )
