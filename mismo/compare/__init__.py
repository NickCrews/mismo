from vaex.dataframe import DataFrame

from mismo._typing import Protocol


class PComparer(Protocol):
    def compare(
        self, datal: DataFrame, datar: DataFrame, links: DataFrame
    ) -> DataFrame:
        ...
