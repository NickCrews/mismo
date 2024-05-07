from __future__ import annotations

from typing import Literal

import ibis
from ibis import _
from ibis import selectors as s
from ibis.expr import types as ir

from mismo import _util
from mismo.block._blocker import EmptyBlocker, PBlocker
from mismo.block._core import block_on_id_pairs


class UnionBlocker:
    """A Blocker that takes the union of the results of other Blockers."""

    def __init__(self, *blockers: PBlocker, labels: bool = False):
        """Create a UnionBlocker from the given Blockers.

        Parameters
        ----------
        blockers
            The Blockers to use.
        labels
            If True, a column of type `array<string>` will be added to the
            resulting table indicating which
            rules caused each record pair to be blocked.
            If False, the resulting table will only contain the columns of left and
            right.
        """
        self.blockers = blockers
        self.labels = labels

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
        **kwargs,
    ) -> ir.Table:
        def blk(blocker: PBlocker):
            j = blocker(left, right, task=task, **kwargs)
            ids = j["record_id_l", "record_id_r"].distinct()
            if self.labels:
                ids = ids.mutate(blocking_rule=ibis.literal(_util.get_name(blocker)))
            return ids

        blockers = self.blockers if self.blockers else [EmptyBlocker()]
        sub_joined = [blk(b) for b in blockers]
        if self.labels:
            result = ibis.union(*sub_joined, distinct=False)
            result = result.group_by(~s.c("blocking_rule")).agg(
                blocking_rules=_.blocking_rule.collect()
            )
            result = result.relocate("blocking_rules", after="record_id_r")
        else:
            result = ibis.union(*sub_joined, distinct=True)
        return block_on_id_pairs(left, right, result)
