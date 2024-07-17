from __future__ import annotations

import sys

import ibis
from ibis import _
import pytest

from mismo import _util


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


def test_sample_table_big_pop_small_sample(table_factory):
    t = table_factory({"v": range(25_000)})
    rowwise_expected = {19799, 24015, 24536}
    rowwise = set(_util.sample_table(t, 5, method="row", seed=42).v.execute())
    blockwise = set(_util.sample_table(t, 5, method="block", seed=42).v.execute())
    blockwise2 = set(_util.sample_table(t, 5, method="block", seed=205).v.execute())
    no_method = set(_util.sample_table(t, 5, seed=42).v.execute())
    assert rowwise == rowwise_expected
    assert no_method == rowwise_expected
    assert blockwise2 != blockwise
    assert len(blockwise2) == 2048
    assert blockwise == set()


def test_sample_table_big_pop_big_sample(table_factory):
    t = table_factory({"v": range(25_000)})
    s1 = set(_util.sample_table(t, 10_000, method="row", seed=42).v.execute())
    s2 = set(_util.sample_table(t, 10_000, method="row", seed=42).v.execute())
    assert len(s1) > 9_000
    assert len(s1) < 11_000
    # It's not a simple range 0, 1, 2, ...
    assert 0 not in s1
    assert 3 in s1
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
