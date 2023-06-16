from __future__ import annotations

from ._base import ComparisonLevel, Weights


def exact(
    column: str,
    name: str | None = None,
    description: str | None = None,
    weights: Weights | None = None,
) -> ComparisonLevel:
    column_left = f"{column}_l"
    column_right = f"{column}_r"
    if name is None:
        name = f"exact_{column}"
    if description is None:
        description = f"Exact match on `{column}`"

    def equals(table):
        return table[column_left] == table[column_right]

    return ComparisonLevel(
        name=name,
        predicate=equals,
        description=description,
        weights=weights,
    )
