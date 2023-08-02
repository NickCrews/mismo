from __future__ import annotations

import ibis
import pandas as pd
import pytest

from mismo.cluster import connected_components


@pytest.mark.parametrize(
    "edges, edges_dtype, expected_components",
    [
        pytest.param(
            [
                (0, 10),
                (1, 10),
                (1, 11),
                (2, 11),
                (2, 12),
                (9, 20),
            ],
            "uint64",
            {
                frozenset({0, 1, 2, 10, 11, 12}),
                frozenset({9, 20}),
            },
            id="linear chain and singleton",
        ),
        pytest.param(
            [
                (0, 10),
                (0, 11),
                (0, 12),
                (0, 13),
                (9, 20),
            ],
            "uint64",
            {
                frozenset({0, 10, 11, 12, 13}),
                frozenset({9, 20}),
            },
            id="hub and singleton",
        ),
        pytest.param(
            [
                ("a", "b"),
                ("a", "c"),
                ("a", "d"),
                ("a", "e"),
                ("x", "y"),
            ],
            "string",
            {
                frozenset({"a", "b", "c", "d", "e"}),
                frozenset({"x", "y"}),
            },
            id="hub and singleton (string keys)",
        ),
        pytest.param(
            [],
            "uint64",
            set(),
            id="empty",
        ),
        pytest.param(
            [(42, 42)],
            "uint64",
            {frozenset({42})},
            id="single self-loop",
        ),
        pytest.param(
            [
                (0, 1),
            ],
            "uint64",
            {frozenset({0, 1})},
            id="single edge",
        ),
    ],
)
def test_connected_components(edges, edges_dtype, expected_components):
    edges_df = pd.DataFrame(
        edges, columns=["record_id_l", "record_id_r"], dtype=edges_dtype
    )
    edges_table = ibis.memtable(edges_df)
    labels = connected_components(edges_table)
    labels = labels.execute()
    component_ids = set(labels.component)
    cid_to_rid = {component_id: set() for component_id in component_ids}
    for row in labels.itertuples():
        cid_to_rid[row.component].add(row.record_id)
    record_components = {frozenset(records) for records in cid_to_rid.values()}
    assert record_components == expected_components
