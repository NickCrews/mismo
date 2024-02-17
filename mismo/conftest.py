from __future__ import annotations

from typing import Callable, Iterable

import ibis
from ibis.expr import types as it
import pytest


@pytest.fixture
def backend() -> ibis.BaseBackend:
    return ibis.duckdb.connect()


_count = 0


@pytest.fixture
def table_factory(backend: ibis.BaseBackend) -> Callable[..., it.Table]:
    def factory(data, schema=None, **kwargs):
        global _count
        name = f"__mismo_test{_count}"
        _count += 1
        mt = ibis.memtable(data, schema=schema)
        result = backend.create_table(name, mt, **kwargs)
        return result

    return factory


@pytest.fixture
def column_factory(table_factory) -> Callable[[Iterable], it.Column]:
    def func(column_data):
        table = table_factory({"column": column_data})
        return table.column

    return func


@pytest.fixture
def t1(table_factory) -> it.Table:
    return table_factory(
        {
            "record_id": [0, 1, 2],
            "letters": ["a", "b", "c"],
            "arrays": [["a", "b"], ["b"], []],
        }
    )


@pytest.fixture
def t2(table_factory) -> it.Table:
    return table_factory(
        {
            "record_id": [90, 91, 92, 93],
            "letters": ["b", "c", "d", None],
            "arrays": [["b"], ["c"], ["d"], None],
        }
    )
