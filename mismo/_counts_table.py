from __future__ import annotations

import functools
from typing import TYPE_CHECKING, NamedTuple

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    import altair as alt


class _HistSpec(NamedTuple):
    n_title: str
    chart_title: str
    chart_subtitle: str


class CountsTable(TableWrapper):
    """A table with at least an Integer column named `n`.

    There will also be variable number of other columns that act as identifiers.

    You won't create this directly, it will be returned to you
    from eg [KeyLinker.key_counts_left][mismo.KeyLinker.key_counts_left],
    [KeyLinker.key_counts_right][mismo.KeyLinker.key_counts_right],
    or [KeyLinker.pair_counts][mismo.KeyLinker.pair_counts].
    """

    n: ir.IntegerColumn
    """The column containing the count."""

    # This MUST be set in subclasses
    _KEY_COUNTS_SPEC: _HistSpec

    @functools.cache
    def n_total(self) -> int:
        """n.sum().fill_null(0), just here for convenience."""
        raw = self.n.sum().execute()
        return int(raw) if raw is not None else 0

    def chart(self, *, n_most_common: int = 50, n_least_common: int = 10) -> alt.Chart:
        return _counts_chart(
            self,
            hist_spec=self._KEY_COUNTS_SPEC,
            n_most_common=n_most_common,
            n_least_common=n_least_common,
        )


class KeyCountsTable(CountsTable):
    _KEY_COUNTS_SPEC = _HistSpec(
        "Number of Records", "Number of Records by Key", "{n_total:_} Total Records"
    )


class PairCountsTable(CountsTable):
    _PAIR_COUNTS_SPEC = _HistSpec(
        "Number of Pairs", "Number of Pairs by Key", "{n_total:_} Total Pairs"
    )


def _counts_chart(
    counts: CountsTable,
    *,
    hist_spec: _HistSpec,
    n_most_common: int,
    n_least_common: int,
):
    import altair as alt

    key_cols = [c for c in counts.columns if c != "n"]
    val = "(" + ibis.literal(", ").join(counts[c].cast(str) for c in key_cols) + ")"
    key_and_n = counts.mutate(val.name("key"), "n")
    key_and_n = key_and_n.filter(_.n > 0)
    most_common = key_and_n.order_by(_.n.desc()).head(n_most_common)
    least_common = key_and_n.order_by(_.n.asc()).head(n_least_common)
    subset = ibis.union(most_common, least_common, distinct=False).cache()
    n_keys_shown = subset.count().execute()
    n_keys_total = key_and_n.count().execute()
    key_title = "(" + ", ".join(key_cols) + ")"
    chart = (
        alt.Chart(subset)
        .properties(
            title=alt.TitleParams(
                hist_spec.chart_title,
                subtitle=[
                    hist_spec.chart_subtitle.format(n_total=counts.n_total()),
                    f"Showing the {n_keys_shown:_} most and least common keys out of {n_keys_total:_}",  # noqa: E501
                ],
                anchor="middle",
            ),
            width=alt.Step(10),
        )
        .mark_bar()
        .encode(
            alt.X("key:O", title=key_title, sort="-y"),
            alt.Y(
                "n:Q",
                title=hist_spec.n_title,
                scale=alt.Scale(type="symlog"),
            ),
            tooltip=[
                alt.Tooltip("n:Q", title=hist_spec.n_title, format=","),
                *[alt.Tooltip(col) for col in key_cols],
            ],
        )
    )
    return chart
