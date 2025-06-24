from __future__ import annotations

from ibis import _
import pytest

from mismo.tests.util import assert_tables_equal
from mismo.types import Diff, Updates


@pytest.fixture
def before(table_factory):
    return table_factory(
        {
            "id": [1, 2, 3, 4],
            "name": ["Alice", None, "Charlie", "David"],
            "age": [10, 20, 30, 40],
        }
    )


@pytest.fixture
def updates_after(table_factory):
    return table_factory(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [10, 20],
        }
    )


@pytest.fixture
def updates(before, updates_after):
    return Updates.from_tables(before, updates_after, join_on="id")


@pytest.fixture
def insertions(table_factory):
    return table_factory(
        {
            "id": [5],
            "name": ["Eve"],
            "age": [50],
        }
    )


@pytest.fixture
def deletions(table_factory):
    return table_factory(
        {
            "id": [3],
            "name": ["Charlie"],
            "age": [30],
        }
    )


@pytest.fixture
def diff(before, updates, insertions, deletions):
    return Diff.from_deltas(
        before=before, updates=updates, insertions=insertions, deletions=deletions
    )


@pytest.fixture
def after(table_factory):
    return table_factory(
        {
            "id": [1, 2, 4, 5],
            "name": ["Alice", "Bob", "David", "Eve"],
            "age": [10, 20, 40, 50],
        }
    )


def test_direct_init_raises():
    """You can only create from the factory methods."""
    with pytest.raises(NotImplementedError):
        Diff(before=None, after=None, insertions=None, deletions=None, updates=None)


def test_from_before_after(after, before, updates, insertions, deletions):
    diff = Diff.from_before_after(before, after, join_on="id")
    expected_updates = updates.filter(_.id.before == 2)  # the id==1 row is unchanged
    assert_tables_equal(before, diff.before(), order_by="id")
    assert_tables_equal(after, diff.after(), order_by="id")
    assert_tables_equal(insertions, diff.insertions(), order_by="id")
    assert_tables_equal(deletions, diff.deletions(), order_by="id")
    assert_tables_equal(expected_updates, diff.updates(), order_by="id")


def test_from_before_after_no_join(after, before):
    diff = Diff.from_before_after(before, after, join_on=False)
    assert diff.updates().count().execute() == 0
    assert_tables_equal(before, diff.before(), order_by="id")
    assert_tables_equal(after, diff.after(), order_by="id")
    assert_tables_equal(after, diff.insertions(), order_by="id")
    assert_tables_equal(before, diff.deletions(), order_by="id")


def test_from_deltas(after, before, updates, insertions, deletions):
    diff = Diff.from_deltas(
        before=before, updates=updates, insertions=insertions, deletions=deletions
    )
    expected_updates = updates.filter(_.id.before == 2)  # the id==1 row is unchanged
    assert_tables_equal(before, diff.before(), order_by="id")
    assert_tables_equal(after, diff.after(), order_by="id")
    assert_tables_equal(insertions, diff.insertions(), order_by="id")
    assert_tables_equal(deletions, diff.deletions(), order_by="id")
    assert_tables_equal(expected_updates, diff.updates(), order_by="id")


def test_from_deltas_noop(before):
    """
    With no deltas, the before and after are the same, and the other tables are empty.
    """
    diff = Diff.from_deltas(before=before)
    assert_tables_equal(before, diff.before(), order_by="id")
    assert_tables_equal(before, diff.after(), order_by="id")
    assert_tables_equal(before.limit(0), diff.insertions(), order_by="id")
    assert_tables_equal(before.limit(0), diff.deletions(), order_by="id")
    assert_tables_equal(
        Updates.from_tables(before, before, join_on="id").limit(0),
        diff.updates(),
        order_by="id",
    )


def test_parquet_roundtrip(diff, tmp_path):
    diff.to_parquets(tmp_path)
    reloaded = Diff.from_parquets(tmp_path)
    assert_tables_equal(diff.before(), reloaded.before(), order_by="id")
    assert_tables_equal(diff.after(), reloaded.after(), order_by="id")
    assert_tables_equal(diff.insertions(), reloaded.insertions(), order_by="id")
    assert_tables_equal(diff.deletions(), reloaded.deletions(), order_by="id")
    assert_tables_equal(diff.updates(), reloaded.updates(), order_by="id")
