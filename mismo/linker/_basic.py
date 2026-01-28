from __future__ import annotations

from typing import Literal

import ibis

from mismo.linkage import _linkage
from mismo.linker import _common, _join_linker


class FullLinker(_common.Linker):
    """
    A [Linker][mismo.Linker] that yields all possible pairs (MxN of them).
    """

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _join_linker.JoinLinker(True, on_slow="ignore", task=task)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._linker.__join_condition__(left, right)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker(left, right)


class EmptyLinker(_common.Linker):
    """A [Linker][mismo.Linker] that yields no pairs."""

    def __init__(self, *, task: Literal["dedupe", "link"] | None = None):
        self.task = task
        self._linker = _join_linker.JoinLinker(False, on_slow="ignore", task=task)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> ibis.ir.BooleanValue:
        return self._linker.__join_condition__(left, right)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        return self._linker(left, right)


class UnnestLinker(_common.Linker):
    """A [Linker][mismo.Linker] that unnests a column before linking.

    We can even block on arrays! For example, first let's split each name into
    significant tokens:

    >>> tokens = _.name.upper().split(" ").filter(lambda x: x.length() > 4)
    >>> t.select(tokens.name("tokens"))
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ tokens                       ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                │
    ├──────────────────────────────┤
    │ ['AGILENT', 'TECHNOLOGIES,'] │
    │ ['NOBEL']                    │
    │ ['NOBEL']                    │
    │ ['ALCATEL']                  │
    │ ['ALCATEL']                  │
    │ ['ALCATEL']                  │
    │ ['CANON', 'EUROPA']          │
    │ ['CANON', 'EUROPA']          │
    │ ['CANON', 'EUROPA']          │
    │ []                           │
    │ …                            │
    └──────────────────────────────┘

    Now, block the tables together wherever two records share a token.
    Note that this blocked `* SCHLUMBERGER LIMITED` with `* SCHLUMBERGER TECHNOLOGY BV`.
    because they both share the `SCHLUMBERGER` token.

    >>> linker = mismo.KeyLinker(tokens.unnest())
    >>> linker(t, t).links.filter(_.name_l != _.name_r).order_by(
    ...     "record_id_l", "record_id_r"
    ... ).head()  # doctest: +SKIP
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                                                     ┃ name_r                                                     ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                                                     │ string                                                     │
    ├─────────────┼─────────────┼────────────┼────────────┼────────────────────────────────────────────────────────────┼────────────────────────────────────────────────────────────┤
    │        2909 │    13390969 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Inc. Netherlands B.V  │
    │        2909 │    13390970 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Inc. Netherlands B.V. │
    │        2909 │    13391015 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherland B.V.       │
    │        2909 │    13391055 │        0.0 │      52.50 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherlands, B.V.     │
    │        2909 │    13391056 │        0.0 │      52.35 │ * AGILENT TECHNOLOGIES, INC.                               │ Hitachi Global Storage Technologies, Netherlands, B.V.     │
    └─────────────┴─────────────┴────────────┴────────────┴────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘
    """

    def __init__(self, column: str, *, task: Literal["dedupe", "link"] | None = None):
        self.column = column
        self.task = task
        self._linker = _join_linker.JoinLinker(self.column, task=task)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        left = left.mutate(left[self.column].unnest().name(self.column))
        right = left.mutate(right[self.column].unnest().name(self.column))
        return self._linker.__call__(left, right)
