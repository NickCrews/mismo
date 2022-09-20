from typing import Iterable, Tuple

import vaex
from vaex.dataframe import DataFrame

from mismo.block import PBlocker
from mismo.fingerprint._fingerprinter import (  # NOQA
    PFingerprinter,
    check_fingerprints,
    is_fingerprinter,
)

FingerprinterPair = Tuple[PFingerprinter, PFingerprinter]
FingerprinterPairsLike = Iterable[FingerprinterPair]


class FingerprintBlocker(PBlocker):
    fingerprinters: list[FingerprinterPair]

    def __init__(self, fingerprinters: FingerprinterPairsLike) -> None:
        self.fingerprinters = convert_fingerprinters(fingerprinters)

    def block(self, datal: DataFrame, datar: DataFrame) -> DataFrame:
        link_chunks = self.links(datal, datar, self.fingerprinters)
        links: DataFrame = vaex.concat(link_chunks)
        links = links.mismo.drop_duplicates()
        links = links.sort(by=["index_left", "index_right"])
        return links

    @staticmethod
    def links(
        data1: DataFrame, data2: DataFrame, fingerprinters: FingerprinterPairsLike
    ) -> DataFrame:
        fps = convert_fingerprinters(fingerprinters)
        links = []
        for fp1, fp2 in fps:
            prints1 = fp1.fingerprint(data1)
            prints2 = fp2.fingerprint(data2)
            link_chunk = merge_fingerprints(prints1, prints2)
            links.append(link_chunk)
        return links


def merge_fingerprints(fpl: DataFrame, fpr: DataFrame) -> DataFrame:
    check_fingerprints(fpl)
    check_fingerprints(fpr)
    non_indexl = [c for c in fpl.column_names if c != "index"]
    non_indexr = [c for c in fpr.column_names if c != "index"]
    fpl["key"] = fpl.mismo.hash_rows(non_indexl)
    fpr["key"] = fpr.mismo.hash_rows(non_indexr)
    links: DataFrame = fpl.join(
        fpr,
        on="key",
        lsuffix="_left",
        rsuffix="_right",
        allow_duplication=True,
    )
    links = links[["index_left", "index_right"]]
    links = links.mismo.drop_duplicates()
    return links


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
