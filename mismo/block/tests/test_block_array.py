from __future__ import annotations

from ibis.expr.types import Table
import pytest


@pytest.fixture
def simple_table(table_factory) -> Table:
    records = [
        (0, ["a", "b"], False),
        (1, ["b"], True),
        (2, ["c"], False),
        (3, ["c", "a", "b"], False),
        (99, [], True),
        (100, None, True),
    ]
    record_ids, strs, bools = zip(*records)
    return table_factory({"record_id": record_ids, "strings": strs, "bools": bools})


@pytest.mark.skip(reason="Not implemented")
def test_block_on_arrays(simple_table: Table):
    from mismo.block import block, block_on_arrays

    rule = block_on_arrays("strings", "strings")
    blocking = block(simple_table, simple_table.view(), rule)
    expected_id_pairs = {
        (0, 0),
        (1, 1),
        (2, 2),
        (3, 3),
        (0, 3),
        (3, 0),
        (0, 1),
        (1, 0),
        (1, 3),
        (3, 1),
        (2, 3),
        (3, 2),
    }
    df = blocking.ids.to_pandas()
    actual_id_pairs = set(df.itertuples(index=False, name=None))
    assert actual_id_pairs == expected_id_pairs
