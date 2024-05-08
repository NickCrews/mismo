from __future__ import annotations

from ibis.expr import types as ir
import pytest

from mismo.block import CrossBlocker, EmptyBlocker, PBlocker
from mismo.tests.util import assert_tables_equal


def test_isinstance():
    assert isinstance(CrossBlocker(), PBlocker)
    assert isinstance(EmptyBlocker(), PBlocker)


def test_cant_instantiate():
    with pytest.raises(TypeError):
        PBlocker()


def test_cross_blocker(table_factory, t1: ir.Table, t2: ir.Table):
    blocked_table = CrossBlocker()(t1, t2)
    blocked_ids = blocked_table["record_id_l", "record_id_r"]
    expected = table_factory(
        {
            "record_id_l": [0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2],
            "record_id_r": [90, 90, 90, 91, 91, 91, 92, 92, 92, 93, 93, 93],
        }
    )
    assert_tables_equal(expected, blocked_ids)


def test_empty_blocker(t1: ir.Table, t2: ir.Table):
    blocked = EmptyBlocker()(t1, t2)
    assert "record_id_l" in blocked.columns
    assert "record_id_r" in blocked.columns
    n = blocked.count().execute()
    assert n == 0
