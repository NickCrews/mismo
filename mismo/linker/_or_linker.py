from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Literal

import ibis

import mismo
from mismo import _upset, joins
from mismo.joins import HasJoinCondition
from mismo.linkage._linkage import Linkage
from mismo.linker._common import Linker, infer_task
from mismo.types._links_table import LinksTable

if TYPE_CHECKING:
    import altair as alt


class OrLinker(Linker):
    """
    A Linker that is the logical OR of multiple [mismo.HasJoinCondition].

    Physically, this is implemented as follows:
    - remove any condition overlap using [mismo.joins.remove_condition_overlap]
    - create a [LinksTable][mismo.LinksTable] for each join condition
      (which should be fast)
    - Union the [LinksTable][mismo.LinksTable]s into a single [LinksTable][mismo.LinksTable]
    """  # noqa: E501

    def __init__(
        self,
        conditions: Iterable[HasJoinCondition] | Mapping[str, HasJoinCondition],
        *,
        on_slow: Literal["error", "warn", "ignore"] = "error",
    ) -> None:
        if isinstance(conditions, Mapping):
            self._join_conditions = {
                k: joins.join_condition(v) for k, v in conditions.items()
            }
        else:
            self._join_conditions = {
                f"condition_{i}": joins.join_condition(c)
                for i, c in enumerate(conditions)
            }
        self.on_slow = on_slow

    @property
    def join_conditions(self) -> dict[str, HasJoinCondition]:
        """
        The tuple of underling HasJoinCondition objects.
        """
        return self._join_conditions

    # We explicitly do not implement __join_condition__ here,
    # because an OR join condition results in inefficient loop joins.

    def __call__(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        task = infer_task(task=None, left=left, right=right)
        if left is right:
            right = right.view()
        if not self._join_conditions:
            return mismo.empty_linkage(left, right)
        conditions = [
            c.__join_condition__(left, right) for c in self._join_conditions.values()
        ]
        for c in conditions:
            joins.check_join_algorithm(left, right, c, on_slow=self.on_slow)
        conditions = joins.remove_condition_overlap(conditions)
        if task == "dedupe":
            conditions = [c & (left.record_id < right.record_id) for c in conditions]
        sub_links = [LinksTable.from_join_condition(left, right, c) for c in conditions]
        links = mismo.UnionTable(sub_links)
        return Linkage(left=left, right=right, links=links)

    def upset_chart(
        self,
        left: ibis.Table,
        right: ibis.Table,
    ) -> alt.Chart:
        import altair as alt

        blocker_names = list(self._join_conditions.keys())
        combos = _upset.combos(blocker_names)

        n_pairs_by_combo = {}
        for combo in combos:
            yes_conditions = [
                self._join_conditions[name].__join_condition__(left, right)
                for name in combo
            ]
            no_conditions = [
                ~self._join_conditions[name].__join_condition__(left, right)
                for name in blocker_names
                if name not in combo
            ]
            condition = ibis.and_(*yes_conditions, *no_conditions)
            n_pairs = mismo.join(left, right, condition).count().execute()
            n_pairs_by_combo[combo] = n_pairs

        import pandas as pd

        intersections_records = []
        for combo, n_pairs in n_pairs_by_combo.items():
            record = {"intersection_size": n_pairs}
            for name in blocker_names:
                record[name] = name in combo
            intersections_records.append(record)

        intersections_df = pd.DataFrame(intersections_records)
        chart = _upset.upset_chart(intersections_df)

        total_pairs = intersections_df.intersection_size.sum()
        title = alt.Title(
            f"{len(blocker_names)} Rules generated {total_pairs:,} total pairs",
            anchor="middle",
        )
        chart = chart.properties(title=title)
        return chart
