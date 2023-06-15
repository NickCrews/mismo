from __future__ import annotations

from ._fs import ComparisonLevel, Condition


class ExactCondition(Condition):
    def __init__(
        self, column: str, name: str | None = None, description: str | None = None
    ):
        column_left = f"{column}_l"
        column_right = f"{column}_r"
        if name is None:
            name = f"exact_{column}"
        if description is None:
            description = f"Exact match on `{column}`"
        super().__init__(
            name=name,
            predicate=lambda table: table[column_left] == table[column_right],
            description=description,
        )


class ExactLevel(ComparisonLevel):
    def __init__(
        self, column: str, name: str | None = None, description: str | None = None
    ):
        condition = ExactCondition(column, name, description)
        super().__init__(condition=condition)
