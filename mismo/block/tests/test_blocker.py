from mismo.block import Equals, FingerprintBlocker
from mismo.datasets import load_febrl1


def test_smoketest():
    df, labels = load_febrl1()
    predicates = [(Equals("surname"), Equals("surname"))]
    blocker = FingerprintBlocker(predicates)
    blocker.block(df, df)
