from __future__ import annotations

from typing import Callable

from ibis.expr.types import Table

from mismo.block._base import _join_blocking


def block_on_arrays(
    left_array_column: str, right_array_column: str
) -> Callable[[Table, Table], Table]:
    """Create a blocking rule for if two arrays share the same value.

    For instance, if your tables contains a column of tags, such as
    `["red", "green"]` in one table and `["green", "blue"]` in the other table.
    You might want to block these two records together because they both contain
    the tag `"green"`.

    Parameters
    ----------
    left_array_column :
        The name of the column in the left table that contains the array.
    right_array_column :
        The name of the column in the right table that contains the array.

    Returns
    -------
    blocking_function:
        A blocking function that takes the left and right tables as inputs and returns
        a table with the columns `record_id_l` and `record_id_r`.

    """

    def block(left: Table, right: Table) -> Table:
        prints_left = left[left_array_column]
        prints_right = right[right_array_column]
        with_prints_left = left.mutate(__mismo_key=prints_left.unnest())
        with_prints_right = right.mutate(__mismo_key=prints_right.unnest())
        result: Table = _join_blocking(
            with_prints_left, with_prints_right, "__mismo_key"
        )["record_id_l", "record_id_r"]
        return result

    return block
