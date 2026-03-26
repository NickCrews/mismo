from __future__ import annotations

from collections.abc import Iterable
from typing import Generic, Literal, TypeVar, cast

from ibis.expr import types as ir
from ibis_enum import IbisEnum

from mismo import _util

IbisEnumT = TypeVar("IbisEnumT", bound=IbisEnum)


class EnumComparer(Generic[IbisEnumT]):
    """
    Assigns an IbisEnum-backed level to record pairs based on one dimension.
    """

    def __init__(
        self,
        name: str,
        levels: type[IbisEnumT],
        cases: Iterable[tuple[ir.BooleanValue | bool, IbisEnumT | int | str]],
        *,
        representation: Literal["string", "integer"] = "integer",
    ):
        self.name = name
        self.levels = levels
        self.cases = tuple((c, self.levels(lev)) for c, lev in cases)
        self.representation = representation

    name: str
    """The name of the comparer, eg "date", "address", "latlon", "price"."""
    levels: type[IbisEnumT]
    """The levels of agreement."""
    cases: tuple[tuple[ir.BooleanValue | bool, IbisEnumT], ...]
    """The cases to check for each level."""
    representation: Literal["string", "integer"] = "integer"
    """The native representation of the levels in ibis expressions.

    Integers are more performant, but strings are more human-readable.
    """

    def __call__(
        self,
        pairs: ir.Table,
        *,
        representation: Literal["string", "integer"] | None = None,
    ) -> ir.Table:
        """Label each record pair with the level that it matches.

        Go through the levels in order. If a record pair matches a level, label ir.
        If none of the levels match a pair, it labeled as "else".

        Parameters
        ----------
        pairs : Table
            A table of record pairs.
        Returns
        -------
        labels : Table
            The input table with an additional column named `self.name` that contains the level that each record pair matches.
        """  # noqa: E501
        if representation is None:
            representation = self.representation

        cases = [
            (cast(ir.BooleanValue | bool, _util.bind_one(pairs, c)), level)
            for c, level in self.cases
        ]
        if representation == "string":
            cases = [(c, level.name) for c, level in cases]
        elif representation == "integer":
            cases = [(c, level.value) for c, level in cases]
        else:
            raise ValueError(f"Invalid representation: {representation}")

        return pairs.mutate(_util.cases(*cases).name(self.name))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, levels=[{self.levels}])"
