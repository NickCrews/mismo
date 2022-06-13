from __future__ import annotations

import pandas as pd
from recordlinkage.datasets import load_febrl1 as _load_febrl1


def load_febrl1() -> tuple[pd.DataFrame, pd.DataFrame]:
    df, links = _load_febrl1(return_links=True)
    index_iloc_mapping = {idx: i for i, idx in enumerate(df.index)}
    df.index = range(len(df))
    link_df: pd.DataFrame = links.to_frame()
    link_df = link_df.reset_index(drop=True)
    link_df.columns = ["index_left", "index_right"]
    link_df["index_left"] = link_df["index_left"].map(index_iloc_mapping)
    link_df["index_right"] = link_df["index_right"].map(index_iloc_mapping)
    return df, link_df
