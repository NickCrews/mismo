from __future__ import annotations

import sys

import duckdb
import ibis
from ibis import _
import pytest

from mismo import _util


def test_select():
    t = ibis.table({"n": "int64", "s": "string"})
    assert _util.select(t).schema() == t.schema()
    assert _util.select(t.n).schema() == ibis.schema({"n": "int64"})
    assert _util.select(t.s, s2=_.s).schema() == ibis.schema(
        {"s": "string", "s2": "string"}
    )
    assert _util.select(t, s2=_.s).schema() == ibis.schema(
        {"n": "int64", "s": "string", "s2": "string"}
    )

    with pytest.raises(ValueError):
        # 0 relations
        _util.select()
    with pytest.raises(ValueError):
        # 0 relations
        _util.select("s", s2=_.s)
    with pytest.raises(ValueError):
        # multipled relations
        _util.select(t.n, t.select("n").n)


@pytest.mark.parametrize(
    "x,expected",
    [
        pytest.param("a", "a", id="str"),
        # TODO: could we submit a patch to ibis to make this look cleaner?
        pytest.param(
            _.a[:2],
            # Before 3.12, slice objects weren't hashable. 3.12+, they are.
            # This affects how ibis reprs the deferred object. An annoying
            # implementation detail, it would be great to backport the 3.12
            # behavior to earlier versions by improving ibis's repr.
            "_.a[slice(None, 2, None)]"
            if sys.version_info >= (3, 12)
            else "Item(_.a, slice(None, 2, None))",
            id="getitem",
        ),
        pytest.param(("a", "b"), "(a, b)", id="str_tuple"),
        pytest.param(_.a, "_.a", id="deferred"),
        pytest.param((_.a, _.b), "(_.a, _.b)", id="deferred_tuple"),
        pytest.param((_.a + 2, _.b), "((_.a + 2), _.b)", id="deffered +"),
        pytest.param([_.a, "b"], "[_.a, b]", id="list"),
        pytest.param([(_.a, "a"), "b"], "[(_.a, a), b]", id="list with tuple"),
    ],
)
def test_get_name(x, expected):
    result = _util.get_name(x)
    assert expected == result


def test_intify_column(table_factory):
    inp = table_factory(
        {
            "vals": ["b", "b", "a", "c", None],
            "ints_expected": [1, 1, 0, 2, 3],
        }
    )
    inp = inp.mutate(original=inp.vals)
    inted, restore = _util.intify_column(inp, "vals")
    assert inted.vals.type() == ibis.dtype("!uint64")
    assert (inted.vals == inted.ints_expected).all().execute()
    restored = restore(inted)
    assert (restored.vals == restored.original).all().execute()


@pytest.mark.xfail(
    duckdb.__version__ == "1.2.0",
    reason="seed is broken in this version of duckdb. https://github.com/duckdb/duckdb/issues/16373",
)
def test_sample_table_big_pop_small_sample(table_factory):
    t = table_factory({"v": range(20_000)})

    rowwise_expected = {
        11213,
        12268,
        14596,
        17735,
        18599,
        19016,
    }
    rowwise = set(_util.sample_table(t, 5, method="row", seed=42).v.execute())
    no_method = set(_util.sample_table(t, 5, seed=42).v.execute())
    assert rowwise == rowwise_expected
    assert no_method == rowwise_expected

    blockwise42_e = _util.sample_table(t, 10_000, method="block", seed=42).v
    blockwise43_e = _util.sample_table(t, 10_000, method="block", seed=43).v
    blockwise42 = set(blockwise42_e.execute())
    blockwise42_2 = set(blockwise42_e.execute())
    blockwise43 = set(blockwise43_e.execute())
    assert blockwise42 == blockwise42_2
    assert blockwise43 != blockwise42
    assert len(blockwise42) == 7712
    assert len(blockwise43) == 8192


def test_sample_table_big_pop_big_sample(table_factory):
    t = table_factory({"v": range(25_000)})
    s1 = set(_util.sample_table(t, 10_000, method="row", seed=42).v.execute())
    s2 = set(_util.sample_table(t, 10_000, method="row", seed=42).v.execute())
    assert len(s1) > 9_000
    assert len(s1) < 11_000
    assert sorted(s1)[:5] != [0, 1, 2, 3, 4]
    assert s1 == s2
    s3 = set(_util.sample_table(t, 10_000, method="row", seed=43).v.execute())
    assert s1 != s3


def test_optional_import():
    with _util.optional_import("foo"):
        import ibis  # noqa: F401

    with pytest.raises(ImportError) as excinfo:
        with _util.optional_import("foo"):
            import does_not_exist  # noqa: F401 # type: ignore

            assert False, "should not get here"
    assert "foo" in str(excinfo.value)

    with pytest.raises(ImportError) as excinfo:
        with _util.optional_import("foo"):
            from does_not_exist import module  # noqa: F401 # type: ignore

            assert False, "should not get here"
    assert "foo" in str(excinfo.value)
