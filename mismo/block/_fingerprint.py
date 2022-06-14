from __future__ import annotations

from typing import Callable, Iterable, Sequence, Union

import numpy as np
import pandas as pd

from mismo._typing import Data, Protocol

# One column "index", is the index of the row in the dataframe.
# The other columns (1 or more), are any values that can be grouped on.
FingerprintFunction = Callable[[Data], pd.DataFrame]
Columns = Union[str, Sequence[str], None]


def check_fingerprints(fingerprints: pd.DataFrame) -> None:
    cols = list(fingerprints.columns)
    if len(cols) < 2:
        raise ValueError("Fingerprints must have at least two columns")
    if "index" not in cols:
        raise ValueError("Fingerprints must have a column named 'index'")
    if cols.count("index") > 1:
        raise ValueError("Fingerprints must have only one column named 'index'")
    if fingerprints["index"].dtype != "int":
        raise ValueError(
            "Fingerprints column 'index' must be of type int. ",
            f"Got {fingerprints['index'].dtype}",
        )


class PFingerprinter(Protocol):
    def fingerprint(self, data: Data) -> pd.DataFrame:
        ...


class ColumnsFingerprinter(PFingerprinter):
    columns: str | list[str] | None

    def __init__(self, columns: Columns = None) -> None:
        if columns is None or isinstance(columns, str):
            self.columns = columns
        else:
            cols = list(columns)
            if not all(isinstance(c, str) for c in cols):
                raise ValueError("Columns must be strings")
            self.columns = cols

    def _select_columns(self, data: Data) -> Data:
        if self.columns is None:
            return data
        else:
            return data[self.columns]

    def _func(self, subset: Data | pd.Series) -> pd.DataFrame:
        raise NotImplementedError()

    def fingerprint(self, data: Data) -> pd.DataFrame:
        return self._func(self._select_columns(data))


class SingleColumnFingerprinter(PFingerprinter):
    def __init__(self, column: str) -> None:
        if not isinstance(column, str):
            raise ValueError("column must be a string")
        self.column = column

    def _func(self, subset: pd.Series) -> pd.DataFrame:
        raise NotImplementedError()

    def fingerprint(self, data: Data) -> pd.DataFrame:
        series = data[self.column]
        return self._func(series)


class FunctionFingerprinter(PFingerprinter):
    def __init__(self, func: FingerprintFunction) -> None:
        self.func = func

    def fingerprint(self, data: Data) -> pd.DataFrame:
        return self.func(data)


class Equals(ColumnsFingerprinter):
    def _func(self, subset: Data | pd.Series) -> pd.DataFrame:
        return add_index(subset)


def add_index(
    values: pd.DataFrame | pd.Series, index: Iterable[int] | None = None
) -> pd.DataFrame:
    if index is None:
        index = np.arange(len(values))
    if values.ndim == 1:
        val_name = getattr(values, "name", None)
        if val_name is None:
            val_name = "value"
        return pd.DataFrame({"index": index, val_name: values})
    elif values.ndim == 2:
        values = pd.DataFrame(values)
        return values.insert(0, "index", index)
    else:
        raise ValueError("values must be 1 or 2 dimensional")
