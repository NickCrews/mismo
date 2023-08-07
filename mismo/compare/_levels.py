from __future__ import annotations

from mismo.compare._comparison import ComparisonLevel


def exact_level(
    column: str,
    name: str | None = None,
    description: str | None = None,
) -> ComparisonLevel:
    """Create a ComparisonLevel that checks for exact matches on a column.

    Parameters
    ----------
    column : str
        The column to check for exact matches. This will get turned into
        the boolean expression `{column}_l = {column}_r`.
    name : str, optional
        The name of the generated level.
    """
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
        condition=equals,
        description=description,
    )
