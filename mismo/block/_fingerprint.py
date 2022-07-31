from __future__ import annotations

from typing import Callable, Iterable, Sequence, Union

import vaex
from vaex.dataframe import DataFrame
from vaex.expression import Expression

from mismo._typing import Protocol

# One column "index", is the index of the row in the dataframe.
# The other columns (1 or more), are any values that can be grouped on.
FingerprintFunction = Callable[[DataFrame], DataFrame]
Columns = Union[str, Sequence[str], None]


def check_fingerprints(fingerprints: DataFrame) -> None:
    cols = list(fingerprints.column_names)
    if len(cols) < 2:
        raise ValueError("Fingerprints must have at least two columns")
    if "index" not in cols:
        raise ValueError("Fingerprints must have a column named 'index'")
    if cols.count("index") > 1:
        raise ValueError("Fingerprints must have only one column named 'index'")
    dtype = fingerprints["index"].dtype
    if dtype not in ("int", "uint"):
        raise ValueError(
            f"Fingerprints column 'index' must be of type uint or int. Got {dtype}"
        )


class PFingerprinter(Protocol):
    def fingerprint(self, data: DataFrame) -> DataFrame:
        ...


def is_fingerprinter(fp):
    return hasattr(fp, "fingerprint") and callable(fp.fingerprint)


class ColumnsFingerprinter(PFingerprinter):
    columns: str | list[str] | None

    def __init__(self, columns: Columns = None) -> None:
        if columns is None:
            self.columns = columns
        elif isinstance(columns, str):
            self.columns = [columns]
        else:
            cols = list(columns)
            if not all(isinstance(c, str) for c in cols):
                raise ValueError("Columns must be strings")
            self.columns = cols

    def _select_columns(self, data: DataFrame) -> DataFrame:
        if self.columns is None:
            return data
        else:
            return data[self.columns]

    def _func(self, subset: DataFrame | Expression) -> DataFrame:
        raise NotImplementedError()

    def fingerprint(self, data: DataFrame) -> DataFrame:
        return self._func(self._select_columns(data))


class SingleColumnFingerprinter(PFingerprinter):
    def __init__(self, column: str) -> None:
        if not isinstance(column, str):
            raise ValueError("column must be a string")
        self.column = column

    def _func(self, subset: Expression) -> DataFrame:
        raise NotImplementedError()

    def fingerprint(self, data: DataFrame) -> DataFrame:
        series = data[self.column]
        return self._func(series)


class FunctionFingerprinter(PFingerprinter):
    def __init__(self, func: FingerprintFunction) -> None:
        self.func = func

    def fingerprint(self, data: DataFrame) -> DataFrame:
        return self.func(data)


class Equals(ColumnsFingerprinter):
    def _func(self, subset: DataFrame | Expression) -> DataFrame:
        return add_index(subset)


def add_index(values: DataFrame, index: Iterable[int] | None = None) -> DataFrame:
    if "index" in values.column_names:
        raise ValueError("Cannot add index to a DataFrame with a column named 'index'")
    result = values.copy()
    if index is None:
        index = vaex.vrange(0, len(result), dtype="uint64")
    result["index"] = index
    return result
