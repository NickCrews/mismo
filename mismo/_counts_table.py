from __future__ import annotations

import functools
from typing import TYPE_CHECKING, NamedTuple

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo.types._wrapper import TableWrapper

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
    _HIST_SPEC: _HistSpec

    @functools.cache
    def n_total(self) -> int:
        """n.sum().fill_null(0), just here for convenience."""
        raw = self.n.sum().execute()
        return int(raw) if raw is not None else 0

    def chart(self) -> alt.Chart:
        return _counts_chart(self, hist_spec=self._HIST_SPEC)


class KeyCountsTable(CountsTable):
    _HIST_SPEC = _HistSpec(
        "Number of Records", "Number of Records by Key", "{n_total:_} Total Records"
    )


class PairCountsTable(CountsTable):
    _HIST_SPEC = _HistSpec(
        "Number of Pairs", "Number of Pairs by Key", "{n_total:_} Total Pairs"
    )


def _counts_chart(counts: CountsTable, *, hist_spec: _HistSpec):
    import altair as alt

    n_total = counts.n_total()
    key_cols = [c for c in counts.columns if c != "n"]
    val = "(" + ibis.literal(", ").join(counts[c].cast(str) for c in key_cols) + ")"
    key_and_n = counts.mutate(val.name("key"), "n")
    key_and_n = key_and_n.filter(_.n > 0)
    frac = key_and_n.n / n_total if n_total > 0 else 0
    key_and_n = key_and_n.mutate(
        frac=frac,
        explanation=(
            "Out of the "
            + f"{n_total:_}, "
            + key_and_n.n.cast(str)
            + " ("
            + (frac * 100).cast(int).cast(str)
            + "%) had the key of "
            + key_and_n.key.cast(str)
        ),
    )
    n_keys_total = key_and_n.count().execute()
    key_title = "(" + ", ".join(key_cols) + ")"
    scrubber_selection = alt.selection_interval(encodings=["x"], empty=True)
    width = 800
    zoomin = (
        alt.Chart(key_and_n, width=width)
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
                alt.Tooltip("frac:Q", title="Fraction", format=".2%"),
                *[alt.Tooltip(col) for col in key_cols],
                alt.Tooltip("explanation", title="Explanation"),
            ],
        )
        .transform_filter(scrubber_selection)
    )
    scrubber = (
        alt.Chart(
            key_and_n,
            width=width,
            height=50,
            title=alt.Title(
                text="<Drag to select>",
                dy=30,
                anchor="middle",
                fontSize=12,
                color="gray",
            ),
        )
        .mark_area(interpolate="step-after")
        .encode(
            alt.X("key:O", sort="-y", axis=None),
            alt.Y("n:Q", title=None, axis=None),
        )
        .add_params(scrubber_selection)
    )
    together = scrubber & zoomin
    together = together.resolve_scale(color="independent")
    together = together.properties(
        title=alt.Title(
            hist_spec.chart_title,
            subtitle=[
                hist_spec.chart_subtitle.format(n_total=n_total),
                f"{n_keys_total:_} keys total",
            ],
            anchor="middle",
        )
    )
    return together
