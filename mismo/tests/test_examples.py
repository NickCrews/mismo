from __future__ import annotations

import pytest

from mismo.block import Blocking
from mismo.examples import load_febrl1, load_febrl2, load_febrl3, load_patents


@pytest.mark.parametrize(
    "load_func, expected_count, expected_link_count",
    [
        (load_febrl1, 1000, 500),
        (load_febrl2, 5000, 1934),
        (load_febrl3, 5000, 6538),
    ],
)
def test_load_febrl_smoketest(load_func, expected_count, expected_link_count):
    blocking: Blocking = load_func()
    assert blocking.dataset_pair.left.count().execute() == expected_count
    assert blocking.blocked_ids.count().execute() == expected_link_count
    repr(blocking)


def test_load_patents_smoketest():
    dataset = load_patents()
    assert dataset.count().execute() == 2379
    repr(dataset)
