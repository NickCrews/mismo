from typing import Protocol, Sequence

import modin.pandas as pd

from mismo._typing import Data, Links
from mismo.block.fingerprint import PFingerprinter, check_fingerprints


class PBlocker(Protocol):
    def block(self, data1: Data, data2: Data) -> Links:
        ...


class FingerprintBlocker(PBlocker):
    fingerprinters: list[PFingerprinter]

    def __init__(
        self, fingerprinters: Sequence[PFingerprinter] | PFingerprinter
    ) -> None:
        if hasattr(fingerprinters, "fingerprint"):
            self.fingerprinters = [fingerprinters]  # type: ignore
        else:
            self.fingerprinters = list(fingerprinters)  # type: ignore

    def block(self, data1: Data, data2: Data) -> Links:
        link_chunks = []
        for fp in self.fingerprinters:
            prints1 = fp.fingerprint(data1)
            prints2 = fp.fingerprint(data2)
            link_chunk = merge_fingerprints(prints1, prints2)
            link_chunks.append(link_chunk)
        links: pd.DataFrame = pd.concat(link_chunks)
        links.drop_duplicates(inplace=True)
        links.sort_values(by=["index_1", "index_2"], inplace=True)
        return links


def merge_fingerprints(fp1: pd.DataFrame, fp2: pd.DataFrame) -> Links:
    check_fingerprints(fp1)
    check_fingerprints(fp2)
    non_index1 = [c for c in fp1.columns if c != "index"]
    non_index2 = [c for c in fp2.columns if c != "index"]
    links: pd.DataFrame = pd.merge(
        fp1,
        fp2,
        left_on=non_index1,
        right_on=non_index2,
        suffixes=("_1", "_2"),
    )
    links = links[["index_1", "index_2"]]
    links = links.drop_duplicates()
    return links
