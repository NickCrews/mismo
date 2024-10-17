from __future__ import annotations

import altair as alt
from ibis import _
from ibis import selectors as s
from ibis.expr import types as ir

from mismo.text import (
    damerau_levenshtein,
    damerau_levenshtein_ratio,
    jaccard,
    jaro_similarity,
    jaro_winkler_similarity,
    levenshtein,
    levenshtein_ratio,
)


def string_comparator_scores(table: ir.Table, col1: str, col2: str) -> ir.Table:
    """Create a table of string comparison measures between two columns.

    This calculates the following similarity measures which range between 0 and 1:
    - The Jaro similarity
    - The Jaro-Winkler similarity
    - The Levenshtein ratio
    - The Damerau-Levenshtein ratio

    as well as the following edit distances:
    - The Levenshtein distance
    - The Damerau-Levenshtein distance


    Parameters
    ----------

    table : ir.Table
        An ibis table containing string columns.
    col1: str
        The name of the first column.
    col2: str
        The name of the second column.

    Returns
    -------
    A table of string comparison measures between two columns.

    Examples
    --------

    >>> import ibis
    >>> from mismo.eda import string_comparator_scores
    >>> ibis.options.interactive = True
    >>> table = ibis.memtable({"string1": ["foo", "bar", "fizz"],
    ... "string2": ["foo", "bam", "fizz buzz"]})
    >>> string_comparator_scores(table, col1="string1", col2="string2")
    ┏━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━┓
    ┃ string1 ┃ string2   ┃ jaro_similarity ┃ jaro_winkler_similarity ┃ … ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━┩
    │ string  │ string    │ float64         │ float64                 │ … │
    ├─────────┼───────────┼─────────────────┼─────────────────────────┼───┤
    │ foo     │ foo       │        1.000000 │                1.000000 │ … │
    │ bar     │ bam       │        0.777778 │                0.822222 │ … │
    │ fizz    │ fizz buzz │        0.814815 │                0.888889 │ … │
    └─────────┴───────────┴─────────────────┴─────────────────────────┴───┘
    """
    comp_table = (
        table.select(_[col1].name("string1"), _[col2].name("string2"))
        .mutate(
            jaro_similarity=jaro_similarity(_.string1, _.string2),
            jaro_winkler_similarity=jaro_winkler_similarity(_.string1, _.string2),
            jaccard_similarity=jaccard(_.string1, _.string2),
            levenshtein_ratio=levenshtein_ratio(_.string1, _.string2),
            damerau_levenshtein_ratio=damerau_levenshtein_ratio(_.string1, _.string2),
            levenshtein_distance=levenshtein(_.string1, _.string2),
            damerau_levenshtein_distance=damerau_levenshtein(_.string1, _.string2),
        )
        .cache()
    )

    return comp_table


def string_comparator_score_chart(table: ir.Table, col1: str, col2: str) -> alt.Chart:
    """Create a heatmap of string comparison measures between two columns.

    Examples
    --------

    >>> import ibis
    >>> from mismo.eda import string_comparator_score_chart
    >>> table = ibis.memtable({"string1": ["foo", "bar", "fizz"],
    ... "string2": ["foo", "bam", "fizz buzz"]})
    >>> string_comparator_score_chart(table, col1="string1", col2="string2")
    alt.Chart(...)
    """

    comp_table = string_comparator_scores(table, col1, col2).mutate(
        strings_to_compare=_.string1.concat(", ", _.string2)
    )
    similarity_records = (
        comp_table.select(
            "strings_to_compare", s.contains("similarity") | s.contains("ratio")
        )
        .pivot_longer(
            ~s.cols("strings_to_compare"), names_to="comparator", values_to="value"
        )
        .mutate(
            comparator=_.comparator.re_replace("(_similarity)|(_ratio)", ""),
        )
    )
    distance_records = (
        comp_table.select("strings_to_compare", s.contains("distance"))
        .pivot_longer(
            ~s.cols("strings_to_compare"), names_to="comparator", values_to="value"
        )
        .mutate(
            comparator=_.comparator.re_replace("_distance", ""),
        )
    )
    base = (
        alt.Chart(similarity_records, title="Similarity")
        .mark_rect()
        .encode(
            y=alt.Text(
                "strings_to_compare:O",
                title="String comparison",
            ),
            x=alt.Text("comparator:O", title=None),
            color=alt.Color("value:Q", legend=None, scale=alt.Scale(domain=(0, 1))),
        )
    )

    text = base.mark_text().encode(
        alt.Text("value:Q", format=".2f"),
        color=alt.value("black"),
    )

    distance_base = (
        alt.Chart(distance_records, title="Distance")
        .mark_rect()
        .encode(
            y=alt.Text("strings_to_compare:O", axis=None),
            x=alt.Text("comparator:O", title=None),
            color=alt.Color("value:Q", legend=None).scale(
                scheme="yelloworangered", reverse=True
            ),
        )
    )

    distance_text = distance_base.mark_text().encode(
        alt.Text("value:Q", format=".2f"),
        color=alt.value("black"),
    )
    chart = alt.hconcat(
        base + text,
        distance_base + distance_text,
        title=alt.Title(text="Heatmaps of string comparison metrics", anchor="middle"),
        config=alt.Config(
            view=alt.ViewConfig(discreteHeight={"step": 30}, discreteWidth={"step": 40})
        ),
    ).resolve_scale(color="independent", size="independent")
    return chart


if __name__ == "__main__":
    import doctest

    doctest.testmod()
