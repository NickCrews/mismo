from __future__ import annotations

from collections import namedtuple
from typing import Any, Iterable, Tuple

import numpy as np
import pandas as pd

from mismo._typing import Data, LabeledLinks, Links, Protocol, Self
from mismo.block._fingerprint import PFingerprinter, check_fingerprints


class PBlocker(Protocol):
    def block(self, data1: Data, data2: Data) -> Links:
        ...


FingerprinterPair = Tuple[PFingerprinter, PFingerprinter]
FingerprinterPairsLike = Iterable[FingerprinterPair]


class FingerprintBlocker(PBlocker):
    fingerprinters: list[FingerprinterPair]

    def __init__(self, fingerprinters: FingerprinterPairsLike) -> None:
        self.fingerprinters = convert_fingerprinters(fingerprinters)

    def block(self, data1: Data, data2: Data) -> Links:
        link_chunks = self.links(data1, data2, self.fingerprinters)
        links: pd.DataFrame = pd.concat(link_chunks)
        links.drop_duplicates(inplace=True)
        links.sort_values(by=["index_left", "index_right"], inplace=True)
        return links

    @staticmethod
    def links(
        data1: Data, data2: Data, fingerprinters: FingerprinterPairsLike
    ) -> Links:
        fps = convert_fingerprinters(fingerprinters)
        links = []
        for fp1, fp2 in fps:
            prints1 = fp1.fingerprint(data1)
            prints2 = fp2.fingerprint(data2)
            link_chunk = merge_fingerprints(prints1, prints2)
            links.append(link_chunk)
        return links


class PBlockLearner(Protocol):
    def fit(self: Self, data1: Data, data2: Data, y: LabeledLinks) -> PBlocker:
        ...


class LinkTranslator:
    def __init__(self, data1: Data, data2: Data) -> None:
        self.modulus = len(data1)

    def links_to_link_ids(self, links: Links) -> pd.Series:
        return links.iloc[0, :] * self.modulus + links.iloc[1, :]

    def link_ids_to_links(self, linkids: pd.Series) -> Links:
        return pd.DataFrame(
            [linkids // self.modulus, linkids % self.modulus],
            columns=["index_left", "index_right"],
        )


class FingerprintBlockLearner(PBlockLearner):
    def __init__(
        self, fingerprinter_candidates: FingerprinterPairsLike, recall: float = 1.0
    ) -> None:
        self.fingerprinter_candidates = convert_fingerprinters(fingerprinter_candidates)
        self.recall = recall

    def fit(self, data1: Data, data2: Data, y: LabeledLinks) -> FingerprintBlocker:
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


SetScore = namedtuple("SetScore", ["set", "new_covers", "n_new_covers", "cost"])


class LinkSet:
    def __init__(self, links: pd.Series, n_extra_covers: int) -> None:
        self.links = links.drop_duplicates()
        self.n_extra_covers = n_extra_covers

    def cost(self, n_new_covers: int) -> float:
        """The cost of adding this set to our solution.

        See `Greedy Set-Cover Algorithms (1974-1979, Chvatal, Johnson, Lovasz, Stein`
        for details. https://www.cs.ucr.edu/~neal/Young08SetCover.pdf

        We choose the set with the lowest cost.

        If two sets both don't add any extra covers, then this seems like a tie.
        But it's not, we want to pick the set that adds the most new covers.
        So in this case the cost is -n_new_covers.

        If we do add extra covers, then we have to think about the "marginal cost",
        ie how much extra covers are we adding per new cover. Therefore the cost is
        n_extra_covers / n_new_covers.
        """
        if n_new_covers == 0:
            return float("inf")
        if self.n_extra_covers == 0:
            return float(-n_new_covers)
        else:
            return self.n_extra_covers / n_new_covers

    def score(self, uncovered: pd.Series) -> SetScore:
        new_covers = np.intersect1d(self.links, uncovered)
        n_new_covers = len(new_covers)
        cost = self.cost(n_new_covers)
        return SetScore(self, new_covers, n_new_covers, cost)

    @classmethod
    def make(cls, links: pd.Series, universe: pd.Series) -> LinkSet:
        links = pd.Series(links).drop_duplicates()
        extra_covers = np.setdiff1d(links, universe)
        n_extra_covers = len(extra_covers)
        return cls(links, n_extra_covers)


def set_cover(universe: pd.Series, sets: list[pd.Series]) -> list[int]:
    """
    Solve the Set Cover Problem.

    Takes a universe of elements to cover (in our case these represent Links)
    and a collection of sets (one set for each fingerprinter, each set is the Links
    that that fingerprinter covers). Returns the indexes of the sets whose union
    is the universe, while minimizing the cost. The cost is the number of links
    covered that aren't part of the universe.
    """
    universe = pd.Series(universe).drop_duplicates()
    candidates = [LinkSet.make(s, universe) for s in sets]

    uncovered = universe
    result: list[int] = []
    while True:
        scores = [c.score(uncovered) for c in candidates]
        if not any(score.n_new_covers for score in scores):
            break
        best_set_idx = np.argmin([score.cost for score in scores])
        result.append(best_set_idx)  # type: ignore[arg-type]
        del candidates[best_set_idx]
        best_set = scores[best_set_idx]
        uncovered = np.setdiff1d(uncovered, best_set.new_covers)
    return result


class PActiveBlockLearner(Protocol):
    def query(
        self, data1: Data, data2: Data, y: LabeledLinks, **kwargs: dict[str, Any]
    ) -> Links:
        """Given the input and labeled links, returns links that should be labeled next.

        Based off of the query() from scikit-activeml.
        https://scikit-activeml.github.io/scikit-activeml-docs/generated/api/skactiveml.pool.RandomSampling.html#skactiveml.pool.RandomSampling.query
        """


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
        suffixes=("_left", "_right"),
    )
    links = links[["index_left", "index_right"]]
    links = links.drop_duplicates()
    return links


def is_fingerprinter(fp):
    return hasattr(fp, "fingerprint")


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
