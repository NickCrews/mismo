from __future__ import annotations

from typing import Iterable

import pyarrow as pa
from vaex.dataframe import DataFrame

from mismo._typing import Protocol


class PBlocker(Protocol):
    """A "Blocker" takes two frames of data, and returns a frame of "links" to compare

    The links are a frame of pairs of indices, where the "index_left" is from the
    first frame, and the second index is from the second frame.
    """

    def block(self, datal: DataFrame, datar: DataFrame) -> PBlocking:
        ...


class PBlocking(Protocol):
    """The result of running Blocker.block().

    Since the result might be larger than memory, we might not want to materialize the
    whole thing. So this class is a wrapper around the result, which can be used to
    materialize the result, or to iterate over it.
    """

    @property
    def n_pairs(self) -> int | None:
        """Return the number of pairs, or None if unknown."""
        pass

    def to_arrow(self) -> pa.Table:
        """Materialize the result as an Arrow table."""
        pass

    def iter_arrow(self, chunk_size: int) -> Iterable[pa.Table]:
        """Iterate over the result as an iterator of Arrow tables."""
        pass
