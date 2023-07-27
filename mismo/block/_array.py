from __future__ import annotations

from ibis.expr.types import Table

from mismo.block._base import _join_blocking


def block_on_arrays(left_array_column: str, right_array_column: str) -> Table:
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
