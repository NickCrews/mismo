from __future__ import annotations

from typing import Protocol

from ibis.expr.types import Table


class PBlocker(Protocol):
    """A ``PBlocker`` determines which pairs of records should be compared.

    Either you can compare a set of records to itself, or you can compare two
    different sets of records.

    Args:
        datal: The left set of records.
        datar: The right set of records. If ``None``, then ``datal`` is compared to
            itself.

    Returns:
        A Table of blocked pairs of records
    """

    def block(self, datal: Table, datar: Table | None = None) -> Table:
        ...
