from __future__ import annotations

import ibis
import pytest

from mismo._datasets import Datasets
from mismo.cluster import degree


@pytest.fixture
def links():
    return ibis.memtable(
        {
            "record_id_l": [0, 1, 1, 2, 2, 9],
            "record_id_r": [10, 10, 11, 11, 12, 20],
        }
    )


def _to_dict(t: ibis.Table) -> dict:
    df = t.order_by("record_id").to_pandas()
    return dict(zip(df["record_id"], df["degree"]))


def test_degree_no_records(links):
    """When records is None, returns a lookup table."""
    result = degree(links=links, records=None)
    assert set(result.columns) == {"record_id", "degree"}
    assert _to_dict(result) == {0: 1, 1: 2, 2: 2, 9: 1, 10: 2, 11: 2, 12: 1, 20: 1}


def test_degree_single_table(links):
    """When records is a single Table, returns the table with degree column."""
    records = ibis.memtable({"record_id": [0, 1, 2, 9, 10, 11, 12, 20, 99]})
    result = degree(links=links, records=records)
    assert isinstance(result, ibis.expr.types.Table)
    assert "degree" in result.columns
    assert "record_id_right" not in result.columns
    d = _to_dict(result)
    assert d[99] == 0  # unlinked record gets 0
    assert d[0] == 1
    assert d[1] == 2


def test_degree_iterable_of_tables(links):
    """When records is an iterable of Tables, returns a Datasets."""
    left = ibis.memtable({"record_id": [0, 1, 2, 9]})
    right = ibis.memtable({"record_id": [10, 11, 12, 20, 99]})
    result = degree(links=links, records=[left, right])
    assert isinstance(result, Datasets)
    assert len(result) == 2
    for t in result.values():
        assert "degree" in t.columns
        assert "record_id_right" not in t.columns
    right_d = _to_dict(result[1])
    assert right_d[99] == 0
    assert right_d[10] == 2


def test_degree_mapping_of_tables(links):
    """When records is a Mapping, returns a Datasets keyed accordingly."""
    a = ibis.memtable({"record_id": [0, 1]})
    b = ibis.memtable({"record_id": [10, 11, 99]})
    result = degree(links=links, records={"a": a, "b": b})
    assert isinstance(result, Datasets)
    assert set(result.keys()) == {"a", "b"}
    assert _to_dict(result["a"]) == {0: 1, 1: 2}
    assert _to_dict(result["b"]) == {10: 2, 11: 2, 99: 0}
