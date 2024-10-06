from __future__ import annotations

import ibis
from ibis.expr import types as ir
import pandas as pd
import pytest

from mismo.cluster import connected_components
from mismo.tests.util import get_clusters


@pytest.fixture(params=["component", "cluster"])
def label_as(request):
    return request.param


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
            "int64",
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
            "int64",
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
            "struct<dataset: string, record_id: int64>",
            {
                frozenset({("a", 0), ("b", 10), ("b", 11), ("b", 12), ("b", 13)}),
                frozenset({("a", 9), ("b", 20)}),
            },
            # this could be useful if different datasets have the same record_id
            id="struct keys",
        ),
        pytest.param(
            [],
            "int64",
            set(),
            id="empty",
        ),
        pytest.param(
            [(42, 42)],
            "int64",
            {frozenset({42})},
            id="single self-loop",
        ),
        pytest.param(
            [(0, 1)],
            "int64",
            {frozenset({0, 1})},
            id="single edge",
        ),
    ],
)
def test_cc_only_edges(
    table_factory, edges_list, label_as, edges_dtype, expected_clusters
):
    """returns a mapping table record_id -> component"""
    schema = {"record_id_l": edges_dtype, "record_id_r": edges_dtype}
    edges_df = pd.DataFrame(edges_list, columns=["record_id_l", "record_id_r"])
    links = table_factory(edges_df, schema=schema).cast(schema)
    labels = connected_components(links=links, label_as=label_as)
    clusters = _labels_to_clusters(labels, label_as)
    assert clusters == expected_clusters


def test_cc_single_records(table_factory, label_as):
    """augments the passed records with a component column"""
    links = table_factory([(0, 1), (1, 2)], columns=["record_id_l", "record_id_r"])
    nodes = table_factory({"record_id": [0, 1, 2, 3]})
    labeled = connected_components(links=links, records=nodes, label_as=label_as)
    clusters = _labels_to_clusters(labeled, label_as)
    assert clusters == {frozenset({0, 1, 2}), frozenset({3})}


def test_cc_no_links_but_records(table_factory, label_as):
    """If there are no links, each record should be its own cluster."""
    link_schema = {"record_id_l": "int64", "record_id_r": "int64"}
    link_df = pd.DataFrame({"record_id_l": [], "record_id_r": []})
    links = table_factory(link_df, schema=link_schema)
    nodes = table_factory({"record_id": [0, 1, 2]})
    labeled = connected_components(links=links, records=nodes, label_as=label_as)
    clusters = _labels_to_clusters(labeled, label_as)
    assert clusters == {frozenset({0}), frozenset({1}), frozenset({2})}


def test_cc_multi_records(table_factory, label_as):
    # multiple input record tables
    links = table_factory([(0, 1), (1, 2)], columns=["record_id_l", "record_id_r"])
    nodes1 = table_factory({"record_id": [0, 1]})
    nodes2 = table_factory({"record_id": [2, 3]})
    l1, l2 = connected_components(
        links=links, records=(nodes1, nodes2), label_as=label_as
    )
    assert set(l1.record_id.execute()) == {0, 1}
    assert set(l2.record_id.execute()) == {2, 3}
    clusters = _labels_to_clusters(ibis.union(l1, l2), label_as)
    assert clusters == {frozenset({0, 1, 2}), frozenset({3})}


def test_cc_max_iterations(table_factory):
    """If we don't give it adequate iterations, it should not give the right result."""
    links = table_factory([(0, 1), (1, 2)], columns=["record_id_l", "record_id_r"])
    labels = connected_components(links=links, max_iter=1)
    clusters = _labels_to_clusters(labels)
    assert clusters != {frozenset({0, 1, 2})}


def _labels_to_clusters(
    labels: ir.Table, label_as: str = "component"
) -> set[frozenset[int]]:
    return get_clusters(labels[label_as], label=labels.record_id)
