from __future__ import annotations

import dataclasses
import os
from typing import Any, Callable, Iterable, Protocol
import uuid
import warnings

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir
import pytest

# we want to have pytest assert introspection in the helpers
pytest.register_assert_rewrite("mismo.tests.util")


def pytest_addoption(parser):
    parser.addoption(
        "--backend",
        action="store",
        default="duckdb",
        help="Specify the backend to use: duckdb (default) or pyspark",
    )


@pytest.fixture
def backend(request) -> ibis.BaseBackend:
    backend_option = request.config.getoption("--backend")
    if backend_option == "duckdb":
        return ibis.duckdb.connect()
    elif backend_option == "pyspark":
        # Suppress warnings from PySpark
        warnings.filterwarnings("ignore", category=UserWarning, module="pyspark")
        warnings.filterwarnings("ignore", category=DeprecationWarning, module="pyspark")
        return ibis.pyspark.connect()
    else:
        raise ValueError(f"Unsupported backend: {backend_option}")


@pytest.fixture
def table_factory(backend: ibis.BaseBackend) -> Callable[..., ir.Table]:
    created_tables = []

    def factory(data, schema=None, columns=None, **kwargs):
        name = f"__mismo_test_{uuid.uuid4().hex}"
        mt = ibis.memtable(data, schema=schema, columns=columns)
        result = backend.create_table(name, mt, **kwargs)
        created_tables.append(name)
        return result

    yield factory

    for name in created_tables:
        backend.drop_table(name, force=True)


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
    yield
    ibis.options = starting_opts
