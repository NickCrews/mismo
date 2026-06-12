from __future__ import annotations

from unittest.mock import Mock

import ibis
import pytest

from mismo._explain import explain
from mismo.exceptions import UnsupportedBackendError


def test_explain_basic():
    con = ibis.duckdb.connect()
    t = con.create_table("t", {"a": [1, 2, 3]})
    result = explain(t)
    assert isinstance(result, str)
    assert len(result) > 0


def test_explain_analyze():
    con = ibis.duckdb.connect()
    t = con.create_table("t", {"a": [1, 2, 3]})
    result = explain(t, analyze=True)
    assert isinstance(result, str)
    assert len(result) > 0


def test_explain_sql_string():
    result = explain("SELECT 1")
    assert isinstance(result, str)
    assert len(result) > 0


def test_explain_no_backend():
    # Build a fake expression that raises AttributeError on _find_backend
    fake = Mock(spec=[])
    with pytest.raises(NotImplementedError, match="must have a backend"):
        explain(fake)


def test_explain_unsupported_backend():
    # explicitly disconnect: a GC'd sqlite3 connection raises an unraisable
    # exception on python 3.13+, which pytest pins on an arbitrary test
    con = ibis.sqlite.connect(":memory:")
    try:
        con.create_table("t", {"a": [1, 2, 3]})
        t = con.table("t")
        with pytest.raises(UnsupportedBackendError):
            explain(t)
    finally:
        con.disconnect()
