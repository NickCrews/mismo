from vaex.dataframe import DataFrame

from mismo.block._learner import LinkTranslator, PBlockLearner, set_cover
from mismo.fingerprint._blocker import (  # NOQA
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

    def fit(
        self, data1: DataFrame, data2: DataFrame, y: DataFrame
    ) -> FingerprintBlocker:
        translator = LinkTranslator(data1, data2)
        pos_links = y[y.iloc[2, :]]
        pos_ids = translator.links_to_link_ids(pos_links)
        candidates = FingerprintBlocker.links(
            data1, data2, self.fingerprinter_candidates
        )
        cand_ids = [translator.links_to_link_ids(c) for c in candidates]
        covering_set = set_cover(pos_ids, cand_ids)
        final_fingerprinters = [self.fingerprinter_candidates[i] for i in covering_set]
        return FingerprintBlocker(final_fingerprinters)
