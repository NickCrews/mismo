from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from typing import Any, Callable, Iterable, Protocol

import ibis
from ibis import _
from ibis.expr import datatypes as dt
from ibis.expr import types as ir
import pytest

# we want to have pytest assert introspection in the helpers
pytest.register_assert_rewrite("mismo.tests.util")

DATA_DIR = Path(__file__).parent.parent / "data"


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


class ColumnFactory(Protocol):
    def __call__(
        column_data: Iterable[Any], type: str | dt.DataType | None = None
    ) -> ir.Column: ...


@pytest.fixture
def column_factory(table_factory) -> ColumnFactory:
    def func(column_data: Iterable[Any], type: str | dt.DataType | None = None):
        if type is not None:
            table = table_factory({"column": column_data}, schema={"column": type})
        else:
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


@dataclasses.dataclass
class ToShape:
    forward: Callable[[ir.Value], ir.Value]
    revert: Callable[[ir.Value], ir.Value]

    def call(self, f: Callable, *args, **kwargs) -> ir.Value:
        first, *rest = args
        args = (self.forward(first), *rest)
        result = f(*args, **kwargs)
        return self.revert(result)


@pytest.fixture(params=["scalar", "column"])
def to_shape(request) -> ToShape:
    """Fixture that allows you to test a function with both scalar and column inputs.

    Say you had some function ``add_one(x: ir.Value) -> ir.Value``.
    You already have a test like

    ```python
    inp = literal(1)
    result = add_one(inp)
    assert result.execute() == 2
    ```

    You can use this fixture to test add_one with a column input:

    ```
    inp = literal(1)
    result = to_shape.revert(add_one(to_shape.forward(inp)))
    assert result.execute() == 2
    ```

    Or, to do it in one step:

    ```
    inp = literal(1)
    result = to_shape.call(add_one, inp)
    assert result.execute() == 2
    ```
    """
    if request.param == "scalar":
        return ToShape(
            forward=lambda x: x,
            revert=lambda x: x,
        )
    elif request.param == "column":
        return ToShape(
            forward=lambda x: ibis.array([x]).unnest(),
            revert=lambda x: x.as_scalar(),
        )
    else:
        assert False


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    # disable color for doctests so we don't have to include escape codes in docstrings
    monkeypatch.setitem(os.environ, "NO_COLOR", "1")
    # Explicitly set the column width to be as large as needed
    monkeypatch.setitem(os.environ, "COLUMNS", "88")
    # reset interactive mode to False for doctests
    starting_opts = ibis.options
    ibis.options.repr.interactive.max_columns = 1000
    ibis.options.repr.interactive.max_depth = 3
    yield
    ibis.options = starting_opts


def download_test_data() -> ir.Table:
    # download test data from https://github.com/NickCrews/apoc-data/releases/tag/20240717-111158
    URL_TEMPLATE = "https://github.com/NickCrews/apoc-data/releases/download/20240717-111158/income_{year}.csv"
    conn = ibis.duckdb.connect()
    sub_tables = [
        conn.read_csv(
            (URL_TEMPLATE.format(year=year) for year in range(2011, 2024)),
            all_varchar=True,
        )
    ]
    t = ibis.union(*sub_tables)
    t = t.select(
        full_address=_.Address
        + ", "
        + _.City
        + ", "
        + _.State
        + ", "
        + _.Zip
        + ", "
        + _.Country
    )
    return t


@pytest.fixture
def addresses_1M(backend: ibis.BaseBackend) -> ir.Table:
    pq = DATA_DIR / "apoc_addresses_1M.parquet"
    if not pq.exists():
        download_test_data().to_parquet(pq)
    t = backend.read_parquet(pq)
    t = t.cache()  # ensure in memory, we don't want to benchmark disk IO
    return t
