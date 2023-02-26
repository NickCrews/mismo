from __future__ import annotations

from typing import Iterable, Tuple

import ibis
from ibis.expr.types import Table

from mismo._dataset import PDataset, PDatasetPair
from mismo.block import Blocking, PBlocker
from mismo.fingerprint._fingerprinter import (
    PFingerprinter,
    is_fingerprinter,
)

FingerprinterPair = Tuple[PFingerprinter, PFingerprinter]
FingerprinterPairsLike = Iterable[FingerprinterPair]


class FingerprintBlocker(PBlocker):
    def __init__(self, fingerprinter_pairs: FingerprinterPairsLike) -> None:
        self._fp_pairs = convert_fingerprinters(fingerprinter_pairs)

    @property
    def fingerprinter_pairs(self) -> list[FingerprinterPair]:
        return self._fp_pairs

    def block(self, dataset_pair: PDatasetPair) -> Blocking:
        left, right = dataset_pair
        joined = join_on_fingerprint_pairs(left, right, self.fingerprinter_pairs)
        id_pairs = joined[left.unique_id_column + "_l", right.unique_id_column + "_r"]
        return Blocking(dataset_pair, id_pairs)


def convert_fingerprinters(fps: FingerprinterPairsLike) -> list[FingerprinterPair]:
    fps_list = list(fps)
    if not fps_list:
        raise ValueError("Fingerprinters must not be empty")
    return [convert_fingerprinter_pair(fp) for fp in fps_list]


def convert_fingerprinter_pair(fp_pair: FingerprinterPair) -> FingerprinterPair:
    pair = tuple(fp_pair)
    if not len(pair) == 2:
        raise ValueError(
            f"Fingerprinters must be a sequence of length 2. Got {pair}",
        )
    if not is_fingerprinter(pair[0]) or not is_fingerprinter(pair[1]):
        raise ValueError(
            f"Fingerprinters must be instances of Fingerprinter. Got {pair}",
        )
    # mypy doesn't understand that pair is length 2.
    return pair  # type: ignore


def join_on_fingerprint_pair(
    left: PDataset, right: PDataset, fpl: PFingerprinter, fpr: PFingerprinter
) -> Table:
    prints_left = fpl.fingerprint(left)
    prints_right = fpr.fingerprint(right)
    with_prints_left = left.table.mutate(__mismo_key=prints_left.unnest())
    with_prints_right = right.table.mutate(__mismo_key=prints_right.unnest())
    result: Table = with_prints_left.inner_join(
        with_prints_right, "__mismo_key", suffixes=("_l", "_r")
    ).drop("__mismo_key")
    return result


def join_on_fingerprint_pairs(
    left: PDataset, right: PDataset, fps: FingerprinterPairsLike
) -> Table:
    chunks = [join_on_fingerprint_pair(left, right, fp1, fp2) for fp1, fp2 in fps]
    if len(chunks) == 0:
        return left.table.cross_join(right.table, suffixes=("_l", "_r")).limit(0)
    elif len(chunks) == 1:
        return chunks[0].distinct()
    else:
        return ibis.union(*chunks, distinct=True)
