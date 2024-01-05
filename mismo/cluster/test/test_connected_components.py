from __future__ import annotations

from typing import Any

from ibis.expr.types import Table
import pandas as pd
import pytest

from mismo.cluster import connected_components


@pytest.mark.parametrize(
    "edges_list, edges_dtype, expected_clusters",
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
            [
                (("a", 0), ("b", 10)),
                (("a", 0), ("b", 11)),
                (("a", 0), ("b", 12)),
                (("a", 0), ("b", 13)),
                (("a", 9), ("b", 20)),
            ],
            "struct<dataset: string, record_id: uint64>",
            {
                frozenset({("a", 0), ("b", 10), ("b", 11), ("b", 12), ("b", 13)}),
                frozenset({("a", 9), ("b", 20)}),
            },
            # this could be useful if different datasets have the same record_id
            id="struct keys",
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
            [(0, 1)],
            "uint64",
            {frozenset({0, 1})},
            id="single edge",
        ),
    ],
)
def test_connected_components(
    table_factory, edges_list, edges_dtype, expected_clusters
):
    edges_df = pd.DataFrame(edges_list, columns=["record_id_l", "record_id_r"])
    schema = {"record_id_l": edges_dtype, "record_id_r": edges_dtype}
    edges_table = table_factory(edges_df, schema=schema)
    labels = connected_components(edges_table)
    clusters = _labels_to_clusters(labels)
    assert clusters == expected_clusters


def test_connected_components_add_missing_nodes(table_factory, column_factory):
    """If a node is not present in the edges table, it would normally be
    missed by the connected components algorithm. But, if we pass it in
    explicitly, it should be included in the output."""
    edges_df = pd.DataFrame([(0, 1), (1, 2)], columns=["record_id_l", "record_id_r"])
    nodes = column_factory([0, 1, 2, 3])
    edges_table = table_factory(edges_df)
    labels = connected_components(edges_table, nodes=nodes)
    clusters = _labels_to_clusters(labels)
    assert clusters == {frozenset({0, 1, 2}), frozenset({3})}


def test_connected_components_max_iterations(table_factory):
    """If we don't give it adequate iterations, it should not give the right result."""
    edges_df = pd.DataFrame([(0, 1), (1, 2)], columns=["record_id_l", "record_id_r"])
    edges_table = table_factory(edges_df)
    labels = connected_components(edges_table, max_iter=1)
    clusters = _labels_to_clusters(labels)
    assert clusters != {frozenset({0, 1, 2})}


def _labels_to_clusters(labels: Table) -> set[frozenset[Any]]:
    df = labels.to_pandas()
    component_ids = set(df.component)
    cid_to_rid = {component_id: set() for component_id in component_ids}
    for row in df.itertuples():
        record_id = row.record_id
        if isinstance(record_id, dict):
            record_id = tuple(record_id.values())
        cid_to_rid[row.component].add(record_id)
    return {frozenset(records) for records in cid_to_rid.values()}
