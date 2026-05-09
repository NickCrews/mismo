from __future__ import annotations

from ibis import _
from ibis.expr import types as ir
import pytest

from mismo.linker import UnnestLinker
from mismo.tests.util import assert_tables_equal


def test_unnest_linker_links_on_any_token(t1: ir.Table, t2: ir.Table):
    """Two records link if any element of their array column matches."""
    linker = UnnestLinker(_.array)
    linkage = linker(t1, t2)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")
    # t1.array: [["a", "b"], ["b"], []]      record_ids 0, 1, 2
    # t2.array: [["b"], ["c"], ["d"], None]  record_ids 90, 91, 92, 93
    # Shared tokens: 0-90 (b), 1-90 (b). Record 2 has no tokens.
    actual = {
        (row["record_id_l"], row["record_id_r"])
        for row in joined_ids.to_pandas().to_dict("records")
    }
    assert actual == {(0, 90), (1, 90)}


def test_unnest_linker_task_link(table_factory):
    """task='link' disables self-join dedupe so every cross-table match shows."""
    left = table_factory(
        {"record_id": [0, 1], "tags": [["a"], ["b"]]}
    )
    right = table_factory(
        {"record_id": [10, 11], "tags": [["a", "b"], ["b"]]}
    )
    linker = UnnestLinker(_.tags, task="link")
    linkage = linker(left, right)
    joined_ids = linkage.links.select("record_id_l", "record_id_r")
    expected = table_factory(
        {"record_id_l": [0, 1, 1], "record_id_r": [10, 10, 11]}
    )
    assert_tables_equal(joined_ids, expected)


def test_unnest_linker_no_matches(table_factory):
    """Records with no shared tokens produce no links."""
    left = table_factory({"record_id": [0, 1], "tags": [["a"], ["b"]]})
    right = table_factory({"record_id": [10, 11], "tags": [["x"], ["y"]]})
    linker = UnnestLinker(_.tags)
    linkage = linker(left, right)
    assert linkage.links.count().execute() == 0


@pytest.mark.parametrize("task", [None, "dedupe", "link"])
def test_unnest_linker_construction(task):
    """Constructor stores config and builds an inner JoinLinker."""
    linker = UnnestLinker(_.tags, task=task)
    assert linker.task == task
    assert linker.column_resolver is not None
    assert linker._linker is not None
