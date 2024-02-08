from __future__ import annotations

from typing import Callable, Iterable

import ibis
from ibis.expr.types import Column, Table
import pytest


@pytest.fixture
def backend() -> ibis.BaseBackend:
    return ibis.duckdb.connect()


_count = 0


@pytest.fixture
def table_factory(backend: ibis.BaseBackend) -> Callable[..., Table]:
    def factory(data, **kwargs):
        global _count
        name = f"__mismo_test{_count}"
        _count += 1
        mt = ibis.memtable(data)
        return backend.create_table(name, mt, **kwargs)

    return factory


@pytest.fixture
def column_factory(table_factory) -> Callable[[Iterable], Column]:
    def func(column_data):
        table = table_factory({"column": column_data})
        return table.column

    return func
