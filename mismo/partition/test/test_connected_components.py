from __future__ import annotations

import ibis
import pandas as pd
import pytest

from mismo.partition._connected_components import connected_components


@pytest.mark.parametrize(
    "edges, expected_components",
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
            {
                frozenset({0, 10, 11, 12, 13}),
                frozenset({9, 20}),
            },
            id="hub and singleton",
        ),
        pytest.param(
            [],
            set(),
            id="empty",
        ),
        pytest.param(
            [
                (0, 1),
            ],
            {
                frozenset({0, 1}),
            },
            id="single edge",
        ),
    ],
)
def test_connected_components(edges, expected_components):
    edges_df = pd.DataFrame(
        edges, columns=["record_id_l", "record_id_r"], dtype="uint64"
    )
    edges_table = ibis.memtable(edges_df)
    left, right = connected_components(edges_table)
    left = left.execute()
    right = right.execute()
    component_ids = set(left.component) | set(right.component)
    cid_to_rid = {component_id: set() for component_id in component_ids}
    for row in left.itertuples():
        cid_to_rid[row.component].add(row.record_id_l)
    for row in right.itertuples():
        cid_to_rid[row.component].add(row.record_id_r)
    record_components = {frozenset(records) for records in cid_to_rid.values()}
    assert record_components == expected_components
