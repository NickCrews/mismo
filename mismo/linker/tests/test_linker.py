from __future__ import annotations

from ibis.expr import types as ir
import pytest

from mismo.linker import EmptyLinker, FullLinker, Linker
from mismo.tests.util import assert_tables_equal


def test_isinstance():
    assert isinstance(EmptyLinker(), Linker)
    assert isinstance(FullLinker(), Linker)


def test_cant_instantiate():
    with pytest.raises(TypeError):
        Linker()


def test_cross_blocker(table_factory, t1: ir.Table, t2: ir.Table):
    linkage = FullLinker()(t1, t2)
    blocked_ids = linkage.links.select("record_id_l", "record_id_r")
    expected = table_factory(
        {
            "record_id_l": [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2],
            "record_id_r": [90, 90, 90, 91, 91, 91, 92, 92, 92, 93, 93, 93],
        }
    )
    assert_tables_equal(expected, blocked_ids)


def test_empty_blocker(t1: ir.Table, t2: ir.Table):
    links = EmptyLinker()(t1, t2).links
    assert "record_id_l" in links.columns
    assert "record_id_r" in links.columns
    n = links.count().execute()
    assert n == 0


def test_empty_blocker_self(t1: ir.Table):
    links = EmptyLinker()(t1, t1).links
    assert "record_id_l" in links.columns
    assert "record_id_r" in links.columns
    n = links.count().execute()
    assert n == 0
