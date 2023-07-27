from __future__ import annotations

import dataclasses
from typing import Literal

from ibis.expr.types import BooleanValue, Table


@dataclasses.dataclass(frozen=True)
class DedupeTask:
    table: Table

    @property
    def n_possible_pairs(self) -> int:
        n = self.table.count().execute()
        return n * (n - 1) // 2

    @staticmethod
    def redundant_comparisons(left: Table, right: Table) -> BooleanValue:
        """The redundant comparisons from the blocking of a dedupe task.

        - we don't want to compare A to A (because that is obviously a match)
        - we only want to compare one of A to B and B to A (because that is redundant)
        """
        return left.record_id >= right.record_id


@dataclasses.dataclass(frozen=True)
class LinkTask:
    left: Table
    right: Table

    @property
    def n_possible_pairs(self) -> int:
        n = self.left.count() * self.right.count()
        return n.execute()

    @staticmethod
    def redundant_comparisons(left: Table, right: Table) -> Literal[False]:
        """
        There are no redundant comparisons in a link task, so this always returns False.
        """
        return False


def check_table(table: Table) -> None:
    """Ensure that a table follows the mismo conventions.

    A table MUST:
      - have a column called "record_id" that is unique
    """
    if "record_id" not in table.columns:
        raise ValueError(
            f"Dataset must have a column called 'record_id', but got {table.columns}"
        )
    if (table.count() != table.distinct(on="record_id").count()).execute():
        raise ValueError("Dataset's 'record_id' column must be unique")
