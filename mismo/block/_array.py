from __future__ import annotations

import dataclasses
from typing import Callable

from ibis.common.deferred import Deferred
from ibis.expr import types as it

from mismo._util import get_column
from mismo.block._block import block


@dataclasses.dataclass(frozen=True)
class ArrayBlocker:
    """Blocks two tables based on array overlap.

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

    >>> from mismo.block import ArrayBlocker, block
    >>> blocker = ArrayBlocker("classes", _.classes.filter(lambda x: x[0] == "A"))
    >>> block(t, t, blocker)
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ classes_l        ┃ classes_r        ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ array<string>    │ array<string>    │
    ├─────────────┼─────────────┼──────────────────┼──────────────────┤
    │        2909 │    29772024 │ ['A61N', 'A61B'] │ ['A61N', 'B32B'] │
    │        2909 │    32288240 │ ['A61N', 'A61B'] │ ['A61D', 'A61B'] │
    │        2909 │    25032591 │ ['A61N', 'A61B'] │ ['A61N', 'A61L'] │
    │        2909 │    32288239 │ ['A61N', 'A61B'] │ ['A61B']         │
    │        2909 │    18321840 │ ['A61N', 'A61B'] │ ['A61F', 'A61N'] │
    │        2909 │    32288235 │ ['A61N', 'A61B'] │ ['A61B']         │
    │        2909 │    18321832 │ ['A61N', 'A61B'] │ ['A61N', 'G09F'] │
    │        2909 │    32288232 │ ['A61N', 'A61B'] │ ['A61B']         │
    │        2909 │    18321710 │ ['A61N', 'A61B'] │ ['A61N', 'F21V'] │
    │        2909 │    26834322 │ ['A61N', 'A61B'] │ ['A61B', 'G11C'] │
    │           … │           … │ …                │ …                │
    └─────────────┴─────────────┴──────────────────┴──────────────────┘
    """

    left_col: str | Deferred | Callable[[it.Table], it.Column]
    """A reference to the column in the left table that contains the array."""

    right_col: str | Deferred | Callable[[it.Table], it.Column]
    """A reference to the column in the right table that contains the array."""

    def __call__(self, left: it.Table, right: it.Table, **kwargs) -> it.Table:
        """Block the two tables based on array overlap."""
        return join_on_arrays(left, right, self.left_col, self.right_col, **kwargs)


def join_on_arrays(
    left: it.Table, right: it.Table, left_col: str, right_col: str, **kwargs
) -> it.Table:
    """Join two tables wherever the arrays in the specified columns overlap."""
    left_array = get_column(left, left_col)
    right_array = get_column(right, right_col)
    with_prints_left = left.mutate(__mismo_key=left_array.unnest())
    with_prints_right = right.mutate(__mismo_key=right_array.unnest())
    result: it.Table = block(
        with_prints_left, with_prints_right, "__mismo_key", **kwargs
    ).drop("__mismo_key_l", "__mismo_key_r")
    return result
