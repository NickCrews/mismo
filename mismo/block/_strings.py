import logging

from ibis.expr.types import ArrayColumn, StringColumn, Table

from mismo.fingerprint._fingerprinter import SingleColumnFingerprinter

logger = logging.getLogger(__name__)


def norm_whitespace(texts: StringColumn) -> StringColumn:
    return texts.strip().re_replace(r"\s+", " ")  # type: ignore


def norm_possessives(texts: StringColumn) -> StringColumn:
    """Fix "jane's house" to "janes house".

    TODO: Also deal with "Ross' house"
    """
    return texts.re_replace(r"(\w)\'s(\b)", r"\1s\2")  # type: ignore


def tokenize(texts: StringColumn) -> ArrayColumn:
    """Returns an column where each element is an array of strings"""
    return norm_whitespace(texts).split(" ")  # type: ignore


class StringBaseFingerprinter(SingleColumnFingerprinter):
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
        strings: StringColumn = data[self.column]
        if self.lower:
            strings = strings.lower()
        if self.norm_possesive:
            strings = norm_possessives(strings)
        if self.norm_whitespace:
            strings = norm_whitespace(strings)
        return strings

    def fingerprint(self, data: Table) -> Table:
        """Selects the column of data, preprocesses it, and passes to _func."""
        data = self._preprocess(data)
        result = self._func(data)
        return result.name(self.name)


class TokenFingerprinter(StringBaseFingerprinter):
    def _func(self, t: StringColumn) -> ArrayColumn:
        return tokenize(t.nullif(""))

    @property
    def name(self) -> str:
        return f"tokens({self.column})"
