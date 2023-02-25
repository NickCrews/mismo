from __future__ import annotations

from typing import Iterable, Tuple

import ibis
from ibis.expr.types import Table

from mismo.block import PBlocker
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

    def block(self, datal: Table, datar: Table | None = None) -> Table:
        if datar is None:
            datar = datal
        chunks = [
            block_on_one_pair(datal, datar, fp1, fp2)
            for fp1, fp2 in self.fingerprinter_pairs
        ]
        if len(chunks) == 0:
            return datal.cross_join(datar, suffixes=("_l", "_r")).limit(0)
        elif len(chunks) == 1:
            return chunks[0].distinct()
        else:
            return ibis.union(*chunks, distinct=True)


def convert_fingerprinters(fps: FingerprinterPairsLike) -> list[FingerprinterPair]:
    fps_list = list(fps)
    if not fps_list:
        raise ValueError("Fingerprinters must not be empty")
    result = []
    for fp in fps_list:
        pair = tuple(fp)
        if not len(pair) == 2:
            raise ValueError(
                f"Fingerprinters must be a sequence of length 2. Got {pair}",
            )
        if not is_fingerprinter(pair[0]) or not is_fingerprinter(pair[1]):
            raise ValueError(
                f"Fingerprinters must be instances of Fingerprinter. Got {pair}",
            )
        result.append(pair)
    # mypy doesn't understand that pair is length 2.
    return result  # type: ignore


def block_on_one_pair(
    data1: Table, data2: Table, fp1: PFingerprinter, fp2: PFingerprinter
) -> Table:
    prints1 = fp1.fingerprint(data1)
    prints2 = fp2.fingerprint(data2)
    with_prints1 = data1.mutate(__mismo_key=prints1.unnest())
    with_prints2 = data2.mutate(__mismo_key=prints2.unnest())
    result: Table = with_prints1.inner_join(
        with_prints2, "__mismo_key", suffixes=("_l", "_r")
    ).drop("__mismo_key")
    return result
