"""An dummy implementatin of PBlocker that just delegates to dedupe"""
from __future__ import annotations

from typing import Iterable, Tuple

import pyarrow as pa
from dedupe import Dedupe, RecordLink
from dedupe._typing import RecordPair
from dedupe.blocking import Fingerprinter
from dedupe.predicates import Predicate
from vaex.dataframe import DataFrame

from mismo.block._blocker import PBlocker, PBlocking


class DedupeBlocker(PBlocker):
    def __init__(self, predicates: Iterable[Predicate], *, in_memory: bool = False):
        self._fingerprinter = Fingerprinter(predicates)
        self._in_memory = in_memory

    def block(
        self, datal: DataFrame, *, datar: DataFrame | None = None
    ) -> IdPairsBlocking:
        if datar is None:
            dd = Dedupe.__new__(Dedupe)
            dd.fingerprinter = self._fingerprinter
            dd.in_memory = self._in_memory
            return IdPairsBlocking.from_pairs(dd.pairs(datal))
        else:
            rl = RecordLink.__new__(RecordLink)
            rl.fingerprinter = self._fingerprinter
            rl.in_memory = self._in_memory
            return IdPairsBlocking.from_pairs(rl.pairs(datal, datar))


IdPair = Tuple[int, int]


class IdPairsBlocking(PBlocking):
    def __init__(self, ids: Iterable[IdPair]) -> None:
        self._ids = ids

    @property
    def n_pairs(self) -> None:
        return None

    def to_arrow(self) -> pa.Table:
        return self._rows_to_table(self._ids)

    def iter_arrow(self, chunk_size: int) -> Iterable[pa.Table]:
        for chunk in _chunk(self._ids, chunk_size):
            yield self._rows_to_table(chunk)

    @classmethod
    def _rows_to_table(cls, rows: Iterable[tuple[int, int]]) -> pa.Table:
        left, right = zip(*rows)
        return pa.Table.from_arrays([left, right], schema=cls.SCHEMA)

    @classmethod
    def from_pairs(cls, pairs: Iterable[RecordPair]) -> IdPairsBlocking:
        ids = ((id1, id2) for (id1, record1), (id2, record2) in pairs)
        return cls(ids)


def _chunk(iterable, n):
    """Chunk an iterable into chunks of size n.

    >>> list(_chunk([1, 2, 3, 4, 5], 2))
    [[1, 2], [3, 4], [5]]
    """
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == n:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
