from __future__ import annotations

import pytest

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
    df, labels = load_func()
    assert df.count().execute() == expected_count
    assert labels.count().execute() == expected_link_count


def test_load_patents_smoketest():
    dataset = load_patents()
    assert dataset.table.count().execute() == 2379
    assert dataset.unique_id_column == "record_id"
    assert dataset.true_label_column == "real_id"
