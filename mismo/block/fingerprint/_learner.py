from __future__ import annotations

from ibis.expr.types import Table

from mismo.block._learner import PBlockLearner
from mismo.block.fingerprint._blocker import (
    FingerprintBlocker,
    FingerprinterPairsLike,
    convert_fingerprinters,
)


class FingerprintBlockLearner(PBlockLearner):
    def __init__(
        self, fingerprinter_candidates: FingerprinterPairsLike, recall: float = 1.0
    ) -> None:
        self.fingerprinter_candidates = convert_fingerprinters(fingerprinter_candidates)
        self.recall = recall

    def fit(  # type: ignore
        self,
        data1: Table,
        data2: Table,
        y: Table,
    ) -> FingerprintBlocker:
        ...
