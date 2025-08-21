from __future__ import annotations

from collections.abc import Iterable
from itertools import combinations
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import altair as alt
    import pandas as pd


def combos(set_names: Iterable[str]) -> frozenset[frozenset[str]]:
    """
    Computes all non-empty subsets of a given iterable of set names.

    Args:
        set_names: An iterable of strings, where each string is a set name.

    Returns:
        A frozenset of frozensets, where each set represents a unique combination
        of the input set names. For example, given ['A', 'B'], it will
        return {{'A'}, {'B'}, {'A', 'B'}}.
    """
    s = list(set_names)
    result = []
    for r in range(1, len(s) + 1):
        result.extend(frozenset(combo) for combo in combinations(s, r))
    return frozenset(result)


assert combos(["A", "B"]) == frozenset(
    {frozenset({"A"}), frozenset({"B"}), frozenset({"A", "B"})}
)
assert combos(["A", "B", "C"]) == frozenset(
    {
        frozenset({"A"}),
        frozenset({"B"}),
        frozenset({"C"}),
        frozenset({"A", "B"}),
        frozenset({"A", "C"}),
        frozenset({"B", "C"}),
        frozenset({"A", "B", "C"}),
    }
)


def upset_chart(data: Any) -> alt.Chart:
    """Generate an UpSet plot.

    Parameters
    ----------
    data
        The data to plot. A Pandas DataFrame or anything that supports the
        __dataframe__ protocol, with each row representing a single
        intersection between sets.
        There should be columns:

        - A column containing the size of the intersection called "intersection_size"
        - A column for each set, with a boolean value indicating whether
          the intersection is in that set.
        - There should be no other columns.

    Returns
    -------
    Chart
        An Altair chart.
    """
    import altair as alt

    df = _to_df(data)
    longer = _pivot_longer(df)
    sets = (
        longer[longer.is_intersect]
        .groupby("set")["intersection_size"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )
    base = alt.Chart(longer)
    intersection_x = alt.X(
        "intersection_id:O",
        axis=None,
        sort=alt.EncodingSortField("intersection_size"),
    )
    set_y = alt.Y(
        "set:O",
        sort=sets,
        axis=alt.Axis(title="Blocking Rule", labelLimit=500),
    )
    p_intersection = _intersection_plot(base, sets, intersection_x)
    p_matrix = _matrix_plot(base, sets, intersection_x, set_y)
    p_sets = _set_plot(base, set_y)
    return alt.vconcat(
        p_intersection,
        alt.hconcat(
            p_matrix,
            p_sets,
            spacing=0,
        ),
        spacing=0,
    )


def _to_df(data) -> pd.DataFrame:
    import pandas as pd

    if isinstance(data, pd.DataFrame):
        return data
    try:
        interchange_object = data.__dataframe__()
    except AttributeError:
        pass
    else:
        return pd.api.interchange.from_dataframe(interchange_object)
    return pd.DataFrame(data)


def _pivot_longer(df: pd.DataFrame) -> pd.DataFrame:
    """Resulting DF has one row for each cell in the matrix.

        intersection_id  intersection_size  intersection_degree                set  is_intersect  set_order
    0                 0                 45                    2       Name First 3          True          0
    1                 1             111258                    2       Name First 3          True          0
    2                 2                 38                    3       Name First 3          True          0
    3                 3               1225                    3       Name First 3          True          0
    4                 4                  2                    2       Name First 3         False          0
    5                 5                906                    3       Name First 3          True          0
    6                 6                  6                    1       Name First 3         False          0
    7                 7               2470                    2       Name First 3          True          0
    8                 8               6340                    1       Name First 3         False          0
    9                 9                646                    2       Name First 3         False          0
    10               10                  2                    3       Name First 3         False          0
    11               11               1495                    4       Name First 3          True          0
    12               12             204224                    1       Name First 3         False          0
    13               13             303104                    1       Name First 3          True          0
    14                0                 45                    2  Coordinates Close         False          1
    15                1             111258                    2  Coordinates Close          True          1
    16                2                 38                    3  Coordinates Close          True          1
    17                3               1225                    3  Coordinates Close          True          1
    18                4                  2                    2  Coordinates Close         False          1
    19                5                906                    3  Coordinates Close         False          1
    20                6                  6                    1  Coordinates Close         False          1
    21                7               2470                    2  Coordinates Close         False          1
    22                8               6340                    1  Coordinates Close         False          1
    23                9                646                    2  Coordinates Close          True          1
    24               10                  2                    3  Coordinates Close          True          1
    25               11               1495                    4  Coordinates Close          True          1
    26               12             204224                    1  Coordinates Close          True          1
    27               13             303104                    1  Coordinates Close         False          1
    28                0                 45                    2    Coauthors Exact          True          2
    29                1             111258                    2    Coauthors Exact         False          2
    30                2                 38                    3    Coauthors Exact          True          2
    31                3               1225                    3    Coauthors Exact         False          2
    32                4                  2                    2    Coauthors Exact          True          2
    33                5                906                    3    Coauthors Exact          True          2
    34                6                  6                    1    Coauthors Exact          True          2
    35                7               2470                    2    Coauthors Exact         False          2
    36                8               6340                    1    Coauthors Exact         False          2
    37                9                646                    2    Coauthors Exact         False          2
    38               10                  2                    3    Coauthors Exact          True          2
    39               11               1495                    4    Coauthors Exact          True          2
    40               12             204224                    1    Coauthors Exact         False          2
    41               13             303104                    1    Coauthors Exact         False          2
    42                0                 45                    2      Classes Exact         False          3
    43                1             111258                    2      Classes Exact         False          3
    44                2                 38                    3      Classes Exact         False          3
    45                3               1225                    3      Classes Exact          True          3
    46                4                  2                    2      Classes Exact          True          3
    47                5                906                    3      Classes Exact          True          3
    48                6                  6                    1      Classes Exact         False          3
    49                7               2470                    2      Classes Exact          True          3
    50                8               6340                    1      Classes Exact          True          3
    51                9                646                    2      Classes Exact          True          3
    52               10                  2                    3      Classes Exact          True          3
    53               11               1495                    4      Classes Exact          True          3
    54               12             204224                    1      Classes Exact         False          3
    55               13             303104                    1      Classes Exact         False          3
    """  # noqa: E501
    import pandas as pd

    sets = [c for c in df.columns if c != "intersection_size"]
    df["intersection_id"] = range(len(df))
    df["intersection_degree"] = df[sets].sum(axis=1)
    longer = pd.melt(
        df, id_vars=["intersection_id", "intersection_size", "intersection_degree"]
    )
    longer = longer.rename(columns={"variable": "set", "value": "is_intersect"})
    set_mapping = {s: i for i, s in enumerate(sets)}
    longer["set_order"] = longer["set"].map(set_mapping)
    return longer


def _intersection_plot(base: alt.Chart, sets: Iterable[str], x) -> alt.Chart:
    import altair as alt

    sets = list(sets)
    intersection_base = base.transform_filter(alt.datum.set == sets[0]).encode(
        x=x,
        y=alt.Y("intersection_size:Q", title="Number of Pairs"),
    )
    bars = intersection_base.mark_bar(color="black")
    text = intersection_base.mark_text(
        dx=5,
        angle=270,
        align="left",
    ).encode(text=alt.Text("intersection_size:Q", format=","))
    layered = bars + text
    return layered


def _matrix_plot(base: alt.Chart, sets: Iterable[str], x, y) -> alt.Chart:
    import altair as alt

    sets = list(sets)
    matrix_circle_bg = base.mark_circle(size=100, color="lightgray", opacity=1).encode(
        x=x,
        y=y,
        tooltip=["set:N"],
    )
    matrix_circle = matrix_circle_bg.mark_circle(
        size=150, color="black", opacity=1
    ).transform_filter(alt.datum.is_intersect)
    layered = alt.layer(matrix_circle_bg, matrix_circle)
    return layered


def _set_plot(base: alt.Chart, y) -> alt.Chart:
    import altair as alt

    set_base = base.transform_filter(alt.datum.is_intersect).encode(
        x=alt.X("sum(intersection_size):Q", title="Number of Pairs"),
        y=y.axis(None),
        tooltip=["set:N"],
    )
    set_bars = set_base.mark_bar().encode(color=alt.Color("set:N", legend=None))
    set_text = set_base.mark_text(dx=5, align="left").encode(
        text=alt.Text("sum(intersection_size):Q", format=",")
    )
    layered = set_bars + set_text
    layered = layered.properties(width=150)
    return layered
