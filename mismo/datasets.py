from __future__ import annotations

from pathlib import Path

import pandas as pd
import vaex
from recordlinkage import datasets as rlds
from vaex.dataframe import DataFrame

from mismo.config import MISMO_HOME


def _wrap_febrl(load_febrl: callable) -> tuple[DataFrame, DataFrame]:
    pdf: pd.DataFrame
    links_multi_index: pd.MultiIndex

    pdf, links_multi_index = load_febrl(return_links=True)
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
    vlinks = vlinks.sort(["index_left", "index_right"])
    return vdf, vlinks


def load_febrl1() -> tuple[DataFrame, DataFrame]:
    return _wrap_febrl(rlds.load_febrl1)


def load_febrl2() -> tuple[DataFrame, DataFrame]:
    return _wrap_febrl(rlds.load_febrl2)


def load_febrl3() -> tuple[DataFrame, DataFrame]:
    return _wrap_febrl(rlds.load_febrl3)


# Don't bother wrapping load_febrl4 because it has a different API,
# could add that later if it's needed.


def _load_or_download(remote: str, cache: Path) -> DataFrame:
    """Load cached parquet file. If doesn't exist, download it from the remote URL."""
    if not cache.exists():
        pdf = pd.read_csv(remote)
        vdf: DataFrame = vaex.from_pandas(pdf)
        cache.parent.mkdir(parents=True, exist_ok=True)
        vdf.export_parquet(cache)
    return vaex.open(cache)


def load_patents() -> tuple[DataFrame, DataFrame]:
    """Load the patents dataset from
    https://github.com/dedupeio/dedupe-examples/tree/master/patent_example
    """
    cache_dir = MISMO_HOME / "datasets/patents"
    data_cache = cache_dir / "data.parquet"
    data_remote = "https://raw.githubusercontent.com/dedupeio/dedupe-examples/master/patent_example/patstat_input.csv"  # noqa E501
    df = _load_or_download(data_remote, data_cache)
    labels_cache = cache_dir / "labels.parquet"
    labels_remote = "https://raw.githubusercontent.com/dedupeio/dedupe-examples/master/patent_example/patstat_reference.csv"  # noqa E501
    labels = _load_or_download(labels_remote, labels_cache)

    # labels looks like
    # |      |   person_id |   leuven_id | person_name                   |
    # |-----:|------------:|------------:|:------------------------------|
    # |    0 |        2909 |      402600 | * AGILENT TECHNOLOGIES, INC.  |
    # |    1 |        3574 |      569309 | * AKZO NOBEL N.V.             |
    # |    2 |        3575 |      569309 | * AKZO NOBEL NV               |
    # |    3 |        3779 |      656303 | * ALCATEL N.V.
    # It's the same length as df, where each row of labels corresponds to the
    # same row of df.
    # Convert labels to links
    labels = labels[["leuven_id"]]
    labels["index"] = vaex.vrange(0, len(labels))
    labels["index"] = labels["index"].astype("uint64")
    links = labels.join(
        labels,
        on="leuven_id",
        lsuffix="_left",
        rsuffix="_right",
        allow_duplication=True,
    )
    links = links[["index_left", "index_right"]]
    links = links.sort(["index_left", "index_right"])

    return df, links
