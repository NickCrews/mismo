from __future__ import annotations

import pandas as pd
import vaex
from recordlinkage.datasets import load_febrl1 as _load_febrl1
from vaex.dataframe import DataFrame


def load_febrl1() -> tuple[DataFrame, DataFrame]:
    pdf: pd.DataFrame
    links_multi_index: pd.MultiIndex

    pdf, links_multi_index = _load_febrl1(return_links=True)
    index_iloc_mapping = {idx: i for i, idx in enumerate(pdf.index)}
    pdf = pdf.reset_index(drop=True)
    vdf = vaex.from_pandas(pdf)
    dtypes = {
        "given_name": "str",
        "surname": "str",
        "street_number": "str",  # keep as string for leading 0s
        "address_1": "str",
        "address_2": "str",
        "suburb": "str",
        "postcode": "str",  # keep as string for leading 0s
        "state": "str",
        "soc_sec_id": "int32",  # 7 digits long, never null, no leading 0s
        "date_of_birth": "str",  # contains some BS dates like 19371233
    }
    for col, dtype in dtypes.items():
        vdf[col] = vdf[col].astype(dtype)

    links: pd.DataFrame = links_multi_index.to_frame(
        index=False, name=["index_left", "index_right"]
    )
    vlinks = vaex.from_pandas(links)
    vlinks["index_left"] = vlinks["index_left"].map(index_iloc_mapping)
    vlinks["index_right"] = vlinks["index_right"].map(index_iloc_mapping)
    return vdf, vlinks
