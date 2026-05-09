from __future__ import annotations

import ibis
import pytest

from mismo import _structs


def _s():
    return ibis.struct({"a": 1, "b": 2, "c": 3})


def test_mutate():
    s = _s()
    result = _structs.mutate(s, b=20, d=4).execute()
    assert result == {"a": 1, "b": 20, "c": 3, "d": 4}


def test_drop():
    s = _s()
    result = _structs.drop(s, "b").execute()
    assert result == {"a": 1, "c": 3}


def test_drop_multiple():
    s = _s()
    result = _structs.drop(s, "a", "c").execute()
    assert result == {"b": 2}


def test_select():
    s = _s()
    result = _structs.select(s, "a", "c").execute()
    assert result == {"a": 1, "c": 3}


def test_rename():
    s = _s()
    result = _structs.rename(s, x="a").execute()
    assert result == {"b": 2, "c": 3, "x": 1}


def test_unpack():
    s = _s()
    vals = list(_structs.unpack(s))
    assert [v.get_name() for v in vals] == ["a", "b", "c"]
    assert [v.execute() for v in vals] == [1, 2, 3]


def test_struct_equal_all_fields():
    s1 = ibis.struct({"a": 1, "b": 2})
    s2 = ibis.struct({"a": 1, "b": 2})
    s3 = ibis.struct({"a": 1, "b": 3})
    assert _structs.struct_equal(s1, s2).execute()
    assert not _structs.struct_equal(s1, s3).execute()


def test_struct_equal_subset_fields():
    s1 = ibis.struct({"a": 1, "b": 2})
    s2 = ibis.struct({"a": 1, "b": 99})
    assert _structs.struct_equal(s1, s2, fields=["a"]).execute()
    assert not _structs.struct_equal(s1, s2, fields=["a", "b"]).execute()


def test_struct_isnull_any():
    s = ibis.struct({"a": 1, "b": ibis.literal(None, type="int64")})
    assert _structs.struct_isnull(s, how="any", fields=["a", "b"]).execute()
    assert not _structs.struct_isnull(s, how="any", fields=["a"]).execute()


def test_struct_isnull_all():
    s_all_null = ibis.struct(
        {"a": ibis.literal(None, type="int64"), "b": ibis.literal(None, type="int64")}
    )
    s_some_null = ibis.struct({"a": 1, "b": ibis.literal(None, type="int64")})
    assert _structs.struct_isnull(s_all_null, how="all", fields=["a", "b"]).execute()
    assert not _structs.struct_isnull(
        s_some_null, how="all", fields=["a", "b"]
    ).execute()


def test_struct_isnull_fields_none():
    s = ibis.struct({"a": 1, "b": 2})
    # fields=None should default to all fields
    assert not _structs.struct_isnull(s, how="any", fields=None).execute()
    assert not _structs.struct_isnull(s, how="all", fields=None).execute()


def test_struct_isnull_invalid_how():
    s = ibis.struct({"a": 1, "b": 2})
    with pytest.raises(ValueError, match="how must be"):
        _structs.struct_isnull(s, how="bogus", fields=None)
