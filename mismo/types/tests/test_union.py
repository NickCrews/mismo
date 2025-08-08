from __future__ import annotations

import ibis
from ibis.common.exceptions import RelationError
import pytest

from mismo.tests.util import assert_tables_equal
from mismo.types import UnionTable


@pytest.fixture
def table1(table_factory):
    return table_factory(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "age": [25, 30, 35],
        }
    )


@pytest.fixture
def table2(table_factory):
    return table_factory(
        {
            "id": [4, 5],
            "name": ["David", "Eve"],
            "age": [40, 45],
        }
    )


@pytest.fixture
def table3(table_factory):
    return table_factory(
        {
            "id": [6],
            "name": ["Frank"],
            "age": [50],
        }
    )


def test_union_table_init(table1, table2):
    union_table = UnionTable([table1, table2])
    assert isinstance(union_table, UnionTable)
    assert len(union_table.tables) == 2
    assert union_table.tables[0] is table1
    assert union_table.tables[1] is table2


def test_union_table_init_empty():
    with pytest.raises(ValueError, match="At least one table must be provided"):
        UnionTable([])


def test_union_table_single_table(table1):
    union_table = UnionTable([table1])
    assert len(union_table.tables) == 1
    assert_tables_equal(union_table, table1)


def test_union_table_multiple_tables(table1, table2, table3, table_factory):
    union_table = UnionTable([table1, table2, table3])
    expected = table_factory(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve", "Frank"],
            "age": [25, 30, 35, 40, 45, 50],
        }
    )
    assert_tables_equal(union_table, expected, order_by="id")


def test_union_table_with_duplicates(table1, table_factory):
    duplicate_table = table_factory(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [25, 30],
        }
    )
    union_table = UnionTable([table1, duplicate_table])
    expected = table_factory(
        {
            "id": [1, 2, 3, 1, 2],
            "name": ["Alice", "Bob", "Charlie", "Alice", "Bob"],
            "age": [25, 30, 35, 25, 30],
        }
    )
    assert_tables_equal(union_table, expected)


def test_union_table_filter(table1, table2):
    union_table = UnionTable([table1, table2])
    filtered = union_table.filter(ibis._.age > 30)
    assert isinstance(filtered, UnionTable)
    assert len(filtered.tables) == 2
    expected = ibis.union(
        table1.filter(ibis._.age > 30), table2.filter(ibis._.age > 30)
    )
    assert_tables_equal(filtered, expected)


def test_union_table_select(table1, table2):
    union_table = UnionTable([table1, table2])
    selected = union_table.select("name", "age")
    assert isinstance(selected, UnionTable)
    assert len(selected.tables) == 2
    assert selected.columns == ("name", "age")


def test_union_table_mutate(table1, table2, table_factory):
    union_table = UnionTable([table1, table2])
    mutated = union_table.mutate(age_plus_10=ibis._.age + 10)
    assert isinstance(mutated, UnionTable)
    assert len(mutated.tables) == 2
    assert "age_plus_10" in mutated.columns
    expected = table_factory(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "age": [25, 30, 35, 40, 45],
            "age_plus_10": [35, 40, 45, 50, 55],
        }
    )
    assert_tables_equal(mutated, expected, order_by="id")


def test_union_table_rename(table1, table2):
    union_table = UnionTable([table1, table2])
    renamed = union_table.rename(full_name="name")
    assert isinstance(renamed, UnionTable)
    assert len(renamed.tables) == 2
    assert "full_name" in renamed.columns
    assert "name" not in renamed.columns


def test_union_table_drop(table1, table2):
    union_table = UnionTable([table1, table2])
    dropped = union_table.drop("age")
    assert isinstance(dropped, UnionTable)
    assert len(dropped.tables) == 2
    assert "age" not in dropped.columns
    assert set(dropped.columns) == {"id", "name"}


def test_union_table_chained_operations(table1, table2, table_factory):
    union_table = UnionTable([table1, table2])
    result = (
        union_table.filter(ibis._.age >= 30)
        .mutate(is_senior=ibis._.age >= 40)
        .select("name", "age", "is_senior")
    )
    assert isinstance(result, UnionTable)
    expected = table_factory(
        {
            "name": ["Bob", "Charlie", "David", "Eve"],
            "age": [30, 35, 40, 45],
            "is_senior": [False, False, True, True],
        }
    )
    assert_tables_equal(result, expected, order_by="age")


def test_union_table_properties_immutable(table1, table2):
    union_table = UnionTable([table1, table2])
    tables = union_table.tables
    assert isinstance(tables, tuple)
    with pytest.raises(AttributeError):
        union_table.tables = ()
    with pytest.raises((TypeError, AttributeError)):
        tables[0] = table1


def test_union_table_schema_consistency(table1, table_factory):
    mismatched_table = table_factory(
        {
            "id": [4, 5],
            "different_name": ["David", "Eve"],  # Different column name
            "age": [40, 45],
        }
    )

    with pytest.raises(RelationError):
        UnionTable([table1, mismatched_table])
