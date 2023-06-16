from __future__ import annotations

import typing

from ._base import PComparisonLevel, Weights


class ExactLevel(PComparisonLevel):
    predicate: typing.ClassVar

    def __init__(
        self,
        column: str,
        name: str | None = None,
        description: str | None = None,
        weights: Weights | None = None,
    ):
        self.column = column
        column_left = f"{column}_l"
        column_right = f"{column}_r"
        if name is None:
            name = f"exact_{column}"
        if description is None:
            description = f"Exact match on `{column}`"
        self.name = name
        self.predicate = lambda table: table[column_left] == table[column_right]  # type: ignore # noqa: E501
        self.description = description
        self.weights = weights

    def set_weights(self, weights: Weights) -> ExactLevel:
        return self.__class__(
            column=self.column,
            name=self.name,
            description=self.description,
            weights=weights,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(column={self.column!r}, weights={self.weights!r})"  # noqa: E501
