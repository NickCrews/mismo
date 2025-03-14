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

    def _pair_counts(self, left: ibis.Table, right: ibis.Table) -> ibis.Table:
        # Need to flesh this API out before I expose it.

        # This assumes all contained blockers are KeyBlockers.
        # Do we want to just rename this UnionBlocker to ManyKeyBlocker?
        # Or formalize an protocol for Blockers like .estimate_n_pairs() -> int?
        def estimate_n_pairs(blocker):
            try:
                return blocker.pair_counts(left, right).n_total
            except AttributeError:
                return None

        return ibis.memtable(
            [
                {"blocker": blocker.name, "n": estimate_n_pairs(blocker)}
                for blocker in self.blockers
            ]
        )

    def _pair_counts_chart(self, left: ibis.Table, right: ibis.Table):
        # Need to flesh out this API before I expose it
        import altair as alt

        counts = self._pair_counts(left, right)
        # some of these counts are 0 (no pairs generated)
        # or None (no way to estimate)
        # At this point leave these in the chart.
        n_title = "Number of Pairs"
        chart = (
            alt.Chart(counts)
            .properties(
                title=alt.TitleParams(
                    "Number of Pairs generated by each Blocker",
                    subtitle=[
                        f"Total number of pairs: {counts.n.sum().execute():,}",
                        "If two blockers generate the same pair, then the pair will only appear once in the blocked output.",  # noqa: E501
                        "So, the sum of these counts is probably an overestimate of the true blocked size.",  # noqa: E501
                    ],
                    anchor="middle",
                ),
                height=150,
            )
            .mark_bar()
            .encode(
                alt.X("blocker:O", title="Blocker", sort="-y"),
                alt.Y("n:Q", title=n_title),
                tooltip=[
                    alt.Tooltip("n:Q", title=n_title, format=","),
                    alt.Tooltip("blocker:O", title="Blocker"),
                ],
            )
        )
        return chart
