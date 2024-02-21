from __future__ import annotations

from typing import Literal

from ibis.expr import types as it

from mismo import _util
from mismo.block._block import _ColumnReferenceLike, join


def join_on_array(
    left: it.Table,
    right: it.Table,
    left_col: _ColumnReferenceLike,
    right_col: _ColumnReferenceLike,
    *,
    on_slow: Literal["error", "warn", "ignore"] = "error",
    task: Literal["dedupe", "link"] | None = None,
    **kwargs,
) -> it.Table:
    """Joins two tables wherever the array columns intersect.

    For instance, if your tables contains a column of tags, such as
    `["red", "green"]` in one table and `["green", "blue"]` in the other table.
    You might want to block these two records together because they both contain
    the tag `"green"`.

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> from mismo.datasets import load_patents
    >>> ibis.options.interactive = True
    >>> t = load_patents().select("record_id", classes=_.classes.split("**")[:2])
    >>> t
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
    ┃ record_id ┃ classes          ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
    │ int64     │ array<string>    │
    ├───────────┼──────────────────┤
    │      2909 │ ['A61N', 'A61B'] │
    │      3574 │ ['G01N', 'B01L'] │
    │      3575 │ ['C09K', 'F17D'] │
    │      3779 │ ['G02B', 'G04G'] │
    │      3780 │ ['H03G', 'B05D'] │
    │      3782 │ ['H04B', 'H01S'] │
    │     15041 │ ['G06F']         │
    │     15042 │ ['G06T', 'G01B'] │
    │     15043 │ ['H04B', 'G06T'] │
    │     25387 │ ['C12N', 'A61K'] │
    │         … │ …                │
    └───────────┴──────────────────┘

    You specify how to get the array columns from the left and right tables.
    Here, for the left table, we keep it simple and just specify the column name.
    For the right table, we show how you can use an Ibis Deferred to calculate
    the column from the right table. You can also use a lambda function.

    What we end up with is all the pairs that share a class, as long
    as that class starts with "A".

    >>> from mismo.block import join_on_array
    >>> join_on_array(t, t, "classes", _.classes.filter(lambda x: x[0] == "A"))
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ classes_l        ┃ classes_r        ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ array<string>    │ array<string>    │
    ├─────────────┼─────────────┼──────────────────┼──────────────────┤
    │       62445 │       66329 │ ['A47J', 'H02K'] │ ['A47J', 'H04N'] │
    │       62445 │       81613 │ ['A47J', 'H02K'] │ ['A47J', 'C08L'] │
    │      317968 │      317972 │ ['A43B', 'A43C'] │ ['A63B', 'A43B'] │
    │      317967 │      317972 │ ['A43B']         │ ['A63B', 'A43B'] │
    │      317970 │      317974 │ ['B32B', 'A63B'] │ ['A63B']         │
    │      317970 │      317975 │ ['B32B', 'A63B'] │ ['B32B', 'A63B'] │
    │      317972 │      317978 │ ['A63B', 'A43B'] │ ['A63B', 'A43B'] │
    │      317965 │      317978 │ ['B32B', 'A63B'] │ ['A63B', 'A43B'] │
    │      317968 │      317981 │ ['A43B', 'A43C'] │ ['A43B']         │
    │      317967 │      317981 │ ['A43B']         │ ['A43B']         │
    │           … │           … │ …                │ …                │
    └─────────────┴─────────────┴──────────────────┴──────────────────┘
    """
    left_array = _util.get_column(left, left_col)
    right_array = _util.get_column(right, right_col)
    left = left.mutate(__mismo_key=left_array.unnest())
    right = right.mutate(__mismo_key=right_array.unnest())
    return join(left, right, "__mismo_key", on_slow=on_slow, task=task, **kwargs).drop(
        "__mismo_key_l", "__mismo_key_r"
    )
