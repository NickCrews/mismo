from mismo.datasets import load_febrl1


def test_load_febrl1_smoketest():
    df, labels = load_febrl1()
    assert len(df) == 1000
    assert len(labels) == 500
