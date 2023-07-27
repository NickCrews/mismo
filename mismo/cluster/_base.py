from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

from ibis.expr.types import Table


@dataclass(frozen=True, kw_only=True)
class Labeling:
    """Holds labeling information for a dataset."""

    table: Table
    labels: Table
    """A table with the columns (record_id, label), where label is a uint64."""

    def with_labels(self, col_name: str = "label") -> Table:
        """The dataset table with the "labels" column added."""
        if col_name in self.table.columns:
            raise ValueError(f"Dataset already has a column named '{col_name}'")
        labels = self.labels.relabel({"label": col_name})
        return self.table.inner_join(labels, "record_id")


class LabelingPair(NamedTuple):
    """Holds a pair of Labeling"""

    left: Labeling
    right: Labeling


def check_labels(t: Table) -> Table:
    if t.columns != ["record_id", "label"]:
        raise ValueError(f"Expected columns ['record_id', 'label'], got {t.columns}")
    if not t.label.type().is_unsigned_integer():
        raise ValueError(
            f"Expected label column to be unsigned integer, got {t.label.type()}"
        )
