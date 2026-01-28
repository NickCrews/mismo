from __future__ import annotations

from typing import Literal

import ibis

from mismo._resolve import IntoValueResolver, value_resolver
from mismo.linkage import _linkage
from mismo.linker import _common, _join_linker


class UnnestLinker(_common.Linker):
    """A [Linker][mismo.Linker] that unnests a column before linking.

    This is useful if you records with sets of tokens that you want to link on,
    for example:
    - splitting names into words/tokens and linking where any token matches.
    - tags, such as product categories, where you want to link where any tag matches.

    This links where ANY of the unnested values match.

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> linkage = mismo.playdata.load_patents()
    >>> t = linkage.left.select("record_id", "name")
    >>> t.head()
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id ┃ name                         ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ uint32    │ string                       │
    ├───────────┼──────────────────────────────┤
    │      2909 │ * AGILENT TECHNOLOGIES, INC. │
    │      3574 │ * AKZO NOBEL N.V.            │
    │      3575 │ * AKZO NOBEL NV              │
    │      3779 │ * ALCATEL N.V.               │
    │      3780 │ * ALCATEL N.V.               │
    └───────────┴──────────────────────────────┘

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

    def __init__(
        self,
        column: IntoValueResolver,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ):
        self.column_resolver = value_resolver(column)
        self.task = task
        self._linker = _join_linker.JoinLinker(self.column_resolver, task=task)

    def __call__(self, left: ibis.Table, right: ibis.Table) -> _linkage.Linkage:
        resolved_left = self.column_resolver(left)
        resolved_right = self.column_resolver(right)
        left = left.mutate(resolved_left.unnest().name(resolved_left.get_name()))
        right = right.mutate(resolved_right.unnest().name(resolved_right.get_name()))
        return self._linker.__call__(left, right)
