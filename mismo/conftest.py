from __future__ import annotations

from typing import Any, Callable, Iterable

import ibis
from ibis.expr.types import Column, Table
import pytest


@pytest.fixture
def backend() -> ibis.BaseBackend:
    return ibis.duckdb.connect()


@pytest.fixture
def table_factory(backend: ibis.BaseBackend) -> Callable[[Any], Table]:
    original = ibis.get_backend()
    ibis.set_backend(backend)

    def func(data):
        return ibis.memtable(data)

    yield func
    ibis.set_backend(original)


@pytest.fixture
def column_factory(table_factory) -> Callable[[Iterable], Column]:
    def func(column_data):
        table = table_factory({"column": column_data})
        return table.column

    return func
