from __future__ import annotations

from typing import Callable, Sequence, Union

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
    columns: Columns

    def fingerprint(self, data: Data) -> pd.DataFrame:
        ...


class BaseFingerprinter(PFingerprinter):
    def __init__(self, columns: Columns) -> None:
        self.columns = columns

    def _select_columns(self, data: Data) -> Data:
        if self.columns is None:
            return data
        else:
            return data[self.columns]


class FunctionFingerprinter(BaseFingerprinter):
    def __init__(self, *, func: FingerprintFunction, columns: Columns = None) -> None:
        self.func = func
        self.columns = columns

    def fingerprint(self, data: Data) -> pd.DataFrame:
        return self.func(self._select_columns(data))


class Equals(BaseFingerprinter):
    def fingerprint(self, data: Data) -> pd.DataFrame:
        return add_range_index(self._select_columns(data))


def add_range_index(values: pd.DataFrame | pd.Series) -> pd.DataFrame:
    index = np.arange(len(values))
    if values.ndim == 1:
        return pd.DataFrame({"index": index, "value": values})
    elif values.ndim == 2:
        values = pd.DataFrame(values)
        return values.insert(0, "index", index)
    else:
        raise ValueError("values must be 1 or 2 dimensional")
