from __future__ import annotations

import ibis
from ibis import _
import pytest

from mismo.types import Updates


@pytest.fixture
def updates(table_factory):
    before = table_factory(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", None, "Charlie", "David"],
            "age": [10, 20, 30, 40],
        }
    )
    after = table_factory(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", None, "Charlie", None],
            "age": [10, 20, 99, 99],
        }
    )
    return Updates.from_tables(before, after, join_on="id")


def test_different_schemas(table_factory):
    before = table_factory({"id": [1, 2, 3]}, schema={"id": "int64"})
    after = table_factory({"id": [1, 2, 3]}, schema={"id": "!int64"})
    with pytest.raises(ValueError):
        Updates.from_tables(before, after, join_on="id")
    u = Updates.from_tables(before, after, join_on="id", check_schemas="names")
    assert dict(u.before().schema()) == dict(ibis.schema({"id": "int64"}))
    assert dict(u.after().schema()) == dict(ibis.schema({"id": "!int64"}))


def test_from_tables_no_join(table_factory):
    before = table_factory({"id": [1, 2]}, schema={"id": "int64"})
    after = table_factory({"id": [3, 4]}, schema={"id": "!int64"})
    u = Updates.from_tables(before, after, join_on=False, check_schemas="names")
    assert u.count().execute() == 0
    assert u.schema() == ibis.schema({"id": "struct<before: int64, after: !int64>"})


def test_any_different(updates: Updates):
    actual = updates.filter(updates.filters.any_different())
    actual_ids = set(actual.after().id.execute())
    expected_ids = {3, 4}
    assert actual_ids == expected_ids


def test_any_different_subset(updates: Updates):
    actual = updates.filter(updates.filters.any_different(["name"]))
    actual_ids = set(actual.after().id.execute())
    expected_ids = {4}
    assert actual_ids == expected_ids


def test_all_different(updates: Updates):
    actual = updates.filter(updates.filters.all_different())
    actual_ids = set(actual.after().id.execute())
    # the ids are the same lol
    expected_ids = set()
    assert actual_ids == expected_ids


def test_all_different_subset(updates: Updates):
    actual = updates.filter(updates.filters.all_different(["name", "age"]))
    actual_ids = set(actual.after().id.execute())
    expected_ids = {4}
    assert actual_ids == expected_ids


def test_filter(updates: Updates):
    f = updates.filter(_.id.before == 3)
    assert isinstance(f, Updates)
    assert f.count().execute() == 1


def test_cache(updates: Updates):
    f = updates.cache()
    assert isinstance(f, Updates)
    assert (f.execute() == updates.execute()).all().all()
