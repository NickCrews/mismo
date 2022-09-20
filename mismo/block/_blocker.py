from __future__ import annotations

from vaex.dataframe import DataFrame

from mismo._typing import Protocol


class PBlocker(Protocol):
    """A "Blocker" takes two frames of data, and returns a frame of "links" to compare

    The links are a frame of pairs of indices, where the "index_left" is from the
    first frame, and the second index is from the second frame.
    """

    def block(self, datal: DataFrame, datar: DataFrame) -> DataFrame:
        ...
