from __future__ import annotations

import ibis
from ibis.common.exceptions import IntegrityError
import pytest

from mismo import joins


def test_join_ensure_named():
    a = ibis.table({"a": "int64", "b": "string"}, name="t")
    b = ibis.table({"a": "int64", "c": "string"}, name="u")
    x = ibis.table({"y": "int64", "z": "string"}, name="u")

    def _schema(t):
        return {k: str(v) for k, v in t.schema().items()}

    assert {
        "a": "int64",
        "b": "string",
        "a_right": "int64",
        "c_right": "string",
    } == _schema(joins.join(a, b, "a", rename_all=True))
    assert {
        "a": "int64",
        "b": "string",
        "a_right": "int64",
        "c_right": "string",
    } == _schema(joins.join(a, b, a.a > b.a, rename_all=True))
    assert {
        "a": "int64",
        "b": "string",
        "y_right": "int64",
        "z_right": "string",
    } == _schema(joins.join(a, x, True, rename_all=True))

    assert {
        "a_l": "int64",
        "b_l": "string",
        "a_right": "int64",
        "c_right": "string",
    } == _schema(joins.join(a, b, a.a > b.a, lname="{name}_l", rename_all=True))
    assert {
        "a_l": "int64",
        "b_l": "string",
        "a": "int64",
        "c": "string",
    } == _schema(
        joins.join(a, b, a.a > b.a, lname="{name}_l", rname="{name}", rename_all=True)
    )

    with pytest.raises(IntegrityError):
        joins.join(a, b, a.a > b.a, lname="{name}_x", rname="{name}_x", rename_all=True)
