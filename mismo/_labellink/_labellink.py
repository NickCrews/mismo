from __future__ import annotations

from typing import Any, Literal

import ibis
from ibis.expr import types as ir

from mismo import _resolve
from mismo._util import join_ensure_named
from mismo.types import Linkage


class LabelLinker:
    """
    WIP: Separate out the easy-to-compute links from records with labels.

    Sometimes, some of your records have a clean ID, such as a social security number.
    These records are easy to link to each other where they match.
    But you still want to link them against records that dont have this label,
    and you want to link records without labels against each other.
    This class helps with that.

    See https://github.com/NickCrews/mismo/issues/77 for more information.
    """  # noqa: E501

    def __init__(self, labels: str | ibis.Deferred | Any) -> None:
        """Create a LabelLinker

        Parameters
        ----------
        labels:
            Something that resolves against the left and right table to the Column
            that holds the labels.
        """
        self.labels = labels

    def definite_linkage(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        return Linkage(
            left=left,
            right=right,
            links=join_ensure_named(
                left,
                right,
                self.labels,
                lname="{name}_l",
                rname="{name}_r",
            ).select("record_id_l", "record_id_r"),
        )

    def indefinite_tables(
        self,
        left: ibis.Table,
        right: ibis.Table,
        *,
        task: Literal["dedupe", "lookup", "link"],
    ) -> tuple[ibis.Table, ibis.Table]:
        if task == "dedupe" or task == "link":
            return left, right
        label_haystack, label_needle = _resolve.resolve_columns(
            self.labels, left, right
        )
        return (
            left,
            right.filter(label_needle.notin(label_haystack)),
        )

    def indefinite_join_condition(
        self, a: ibis.Table, b: ibis.Table
    ) -> ir.BooleanColumn:
        label_a, label_b = _resolve.resolve_columns(self.labels, a, b)
        return ibis.or_(
            label_a != label_b,
            label_a.isnull(),
            label_b.isnull(),
        )
