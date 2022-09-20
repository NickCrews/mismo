from __future__ import annotations

from typing import Iterable

import pyarrow as pa
from vaex.dataframe import DataFrame

from mismo._typing import Protocol


class PBlocker(Protocol):
    """A ``PBlocker`` determines which pairs of records should be compared.

    Either you can compare a set of records to itself, or you can compare two
    different sets of records.

    Args:
        datal: The left set of records.
        datar: The right set of records. If ``None``, then ``datal`` is compared to
            itself.

    Returns:
        A ``PBlocking`` object, which can be used to materialize the result, or to
        iterate over it.
    """

    def block(self, datal: DataFrame, *, datar: DataFrame | None = None) -> PBlocking:
        ...


class PBlocking(Protocol):
    """A collection of pairs of records that should be compared.

    Since the result might be larger than memory, we might not want to materialize the
    whole thing. So this class is a wrapper around the result, which can be used to
    materialize the result, or to iterate over it.
    """

    SCHEMA: pa.Schema = pa.schema(
        [
            pa.field("left", pa.int32()),
            pa.field("right", pa.int32()),
        ]
    )

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
