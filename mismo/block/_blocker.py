from __future__ import annotations

from typing import Protocol, runtime_checkable

from ibis.expr.types import Table

from mismo._dataset import PDataset, PDatasetPair


@runtime_checkable
class PBlocking(Protocol):
    """Contains blocking results"""

    @property
    def dataset_pair(self) -> PDatasetPair:
        """The DatasetPair that was blocked."""

    @property
    def id_pairs(self) -> Table:
        """A table of (left_id, right_id) pairs that should be compared."""

    @property
    def data_pairs(self) -> Table:
        """The dataset pair joined together on the id_pairs"""
        left, right = self.dataset_pair.left, self.dataset_pair.right
        return join_datasets(left, right, self.id_pairs)


class Blocking(PBlocking):
    def __init__(self, dataset_pair: PDatasetPair, id_pairs: Table):
        self._dataset_pair = dataset_pair
        self._id_pairs = id_pairs

    @property
    def dataset_pair(self) -> PDatasetPair:
        return self._dataset_pair

    @property
    def id_pairs(self) -> Table:
        return self._id_pairs


@runtime_checkable
class PBlocker(Protocol):
    """A ``PBlocker`` determines which pairs of records should be compared.

    Either you can compare a set of records to itself, or you can compare two
    different sets of records.

    Args:
        dataset_pair: The DatasetPair to block

    Returns:
        A ``Blocking`` object containing the results of the blocking.
    """

    def block(self, dataset_pair: PDatasetPair) -> PBlocking:
        ...


def join_datasets(left: PDataset, right: PDataset, on: Table) -> Table:
    """Join two datasets together, so that we can compare them."""
    check_id_pairs(on)
    return on.join(left.table, left.unique_id_column, suffixes=("", "_l")).join(
        right.table, right.unique_id_column, suffixes=("", "_r")
    )


def check_id_pairs(id_pairs: Table) -> None:
    """Check that the id pairs are valid."""
    if len(id_pairs.columns) != 2:
        raise ValueError(
            f"Expected id_pairs to have 2 columns, but it has {id_pairs.columns}"
        )
