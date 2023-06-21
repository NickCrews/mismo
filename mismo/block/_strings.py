from __future__ import annotations

import logging

from ibis.expr.types import (
    ArrayColumn,
    ArrayValue,
    Column,
    StringColumn,
    StringValue,
    Table,
)

from mismo.block.fingerprint._fingerprinter import SingleColumnFingerprinter

logger = logging.getLogger(__name__)


def norm_whitespace(texts: StringValue) -> StringValue:
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore


def norm_possessives(texts: StringValue) -> StringValue:
    """Fix "jane's house" to "janes house".

    TODO: Also deal with "Ross' house"
    """
    return texts.re_replace(r"(\w)\'s(\b)", r"\1s\2")  # type: ignore


def tokenize(texts: StringValue) -> ArrayValue:
    """Returns an column where each element is an array of strings"""
    return norm_whitespace(texts).split(" ")


class StringColumnFingerprinter(SingleColumnFingerprinter):
    def __init__(
        self,
        column: str,
        lower: bool = True,
        norm_possesive: bool = True,
        norm_whitespace: bool = True,
    ) -> None:
        super().__init__(column=column)
        self.lower = lower
        self.norm_possesive = norm_possesive
        self.norm_whitespace = norm_whitespace

    def _preprocess(self, data: Table) -> StringColumn:
        strings = data[self.column]
        if self.lower:
            strings = strings.lower()
        if self.norm_possesive:
            strings = norm_possessives(strings)
        if self.norm_whitespace:
            strings = norm_whitespace(strings)
        return strings  # type: ignore

    def fingerprint(self, table: Table) -> ArrayColumn:
        """Selects the column of data, preprocesses it, and passes to _func."""
        string_col = self._preprocess(table)
        result = self._func(string_col)
        return result.name(self.name)  # type: ignore


class TokenFingerprinter(StringColumnFingerprinter):
    # TODO: probably StringColumnFingerprinter shouldn't inherit from
    # SingleColumnFingerprinter, because _func in the parent accepts a Column
    # but the child only accepts a StringColumn (AKA it's more restrictive).
    # this violates Liskov's substitution principle, which mypy complains about.
    def _func(self, t: Column) -> ArrayColumn:
        return tokenize(t.nullif(""))  # type: ignore

    @property
    def name(self) -> str:
        return f"tokens({self.column})"
