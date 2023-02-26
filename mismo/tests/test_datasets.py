from __future__ import annotations

from mismo.examples import load_febrl1


def test_load_febrl1_smoketest():
    df, labels = load_febrl1()
    assert df.count().execute() == 1000
    assert labels.count().execute() == 500
