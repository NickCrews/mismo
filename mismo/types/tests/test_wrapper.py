from __future__ import annotations

import ibis

from mismo.tests.util import assert_equal
from mismo.types._wrapper import StructWrapper, TableWrapper


def test_table_wrapper_basic(table_factory):
    table = table_factory({"a": [1, 2], "b": [3, 4], "_x": [5, 6]})
    wrapped = TableWrapper(table)
    assert isinstance(wrapped, TableWrapper)
    assert isinstance(wrapped, ibis.Table)
    assert wrapped.schema() == table.schema()
    assert wrapped.columns == table.columns
    assert_equal(wrapped.a, table.a)
    assert_equal(wrapped.b, table.b)
    assert_equal(wrapped["_x"], table["_x"])


def test_struct_wrapper_basic(table_factory):
    table = table_factory({"a": [1, 2], "_x": [5, 6]})
    struct_col = ibis.struct({"a": table.a, "b": 5, "_x": table["_x"]})
    wrapped = StructWrapper(struct_col)
    assert isinstance(wrapped, StructWrapper)
    assert isinstance(wrapped, ibis.Value)
    assert wrapped.type() == struct_col.type()
    assert wrapped.fields == struct_col.fields
    assert_equal(wrapped.a, struct_col.a)
    assert_equal(wrapped.b, struct_col.b)
    assert_equal(wrapped["_x"], struct_col["_x"])
    assert wrapped.to_list() == struct_col.to_list()
