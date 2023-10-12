from __future__ import annotations

import pytest

from mismo.datasets import load_febrl1, load_febrl2, load_febrl3, load_patents


@pytest.mark.parametrize(
    "load_func, expected_count, expected_link_count",
    [
        (load_febrl1, 1000, 500),
        (load_febrl2, 5000, 1934),
        (load_febrl3, 5000, 6538),
    ],
)
def test_load_febrl_smoketest(load_func, expected_count, expected_link_count):
    data, links = load_func()
    assert data.count().execute() == expected_count
    assert links.count().execute() == expected_link_count
    repr(data)


def test_load_patents_smoketest():
    dataset = load_patents()
    assert dataset.count().execute() == 2379
    repr(dataset)
