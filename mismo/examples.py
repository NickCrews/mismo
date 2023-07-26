from __future__ import annotations

from typing import Callable

import ibis
from ibis import _
from ibis.expr.types import Table
import pandas as pd
from recordlinkage import datasets as rlds

from mismo.block import Blocking


def _wrap_febrl(
    load_febrl: Callable[..., tuple[pd.DataFrame, pd.MultiIndex]]
) -> Blocking:
    pdf, links_multi_index = load_febrl(return_links=True)
    pdf = pdf.reset_index(drop=False)
    con = ibis.duckdb.connect()
    con.create_table("data", pdf)
    t = con.table("data")
    dtypes = {
        "rec_id": "str",
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
    t = t.mutate(**{col: t[col].cast(dtype) for col, dtype in dtypes.items()})
    t = t.relabel({"rec_id": "record_id"})
    t = t.order_by("record_id")
    t = t.cache()

    links_df = links_multi_index.to_frame(
        index=False, name=["record_id_l", "record_id_r"]
    )
    con.create_table("links", links_df)
    links = con.table("links")
    links = links.order_by(["record_id_l", "record_id_r"])
    links = links.cache()
    return Blocking(t, t.view(), blocked_ids=links)


def load_febrl1() -> Blocking:
    return _wrap_febrl(rlds.load_febrl1)  # pyright: ignore


def load_febrl2() -> Blocking:
    return _wrap_febrl(rlds.load_febrl2)  # pyright: ignore


def load_febrl3() -> Blocking:
    return _wrap_febrl(rlds.load_febrl3)  # pyright: ignore


# Don't bother wrapping load_febrl4 because it has a different API,
# could add that later if it's needed.


def load_patents() -> Table:
    """Load the patents dataset from
    https://github.com/dedupeio/dedupe-examples/tree/master/patent_example

    This represents a dataset of patents, and the task is to determine which
    patents came from the same inventor.

    Returns
    -------
    An Ibis Table with the following schema:
        - record_id: int64
          A unique ID for each row in the table.
        - label_true: int64
          The manually labeled, true ID of the inventor.
        - name_true: str
          The manually labeled, true name of the inventor.
        - name: str
          The raw name on the patent.
        - latitude: float64
          Geocoded from the inventor's address. 0.0 indicates no address was found
        - longitude: float64
        - coauthor: str
          A list of coauthors on the patent, separated by **
        - class_: str
          A list of 4-digit IPC technical codes, separated by **
    """
    data_remote = "https://raw.githubusercontent.com/dedupeio/dedupe-examples/master/patent_example/patstat_input.csv"  # noqa E501
    labels_remote = "https://raw.githubusercontent.com/dedupeio/dedupe-examples/master/patent_example/patstat_reference.csv"  # noqa E501
    t = ibis.read_csv(data_remote)
    labels = ibis.read_csv(labels_remote)

    # Data looks like
    # ┏━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓ # noqa E501
    # ┃ person_id ┃ Lat     ┃ Lng      ┃ Coauthor                   ┃ Name                 ┃ Class                      ┃ # noqa E501
    # ┡━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩ # noqa E501
    # │ int64     │ float64 │ float64  │ string                     │ string               │ string                     │ # noqa E501
    # ├───────────┼─────────┼──────────┼────────────────────────────┼──────────────────────┼────────────────────────────┤ # noqa E501
    # │      2909 │    0.00 │ 0.000000 │ KONINK PHILIPS ELECTRONIC… │ AGILENT TECHNOLOGIES │ A61N**A61B                 │ # noqa E501
    # │      3574 │    0.00 │ 0.000000 │ TSJERK  HOEKSTRA**ANDRESS… │ AKZO NOBEL           │ G01N**B01L**C11D**G02F**F… │ # noqa E501
    # │      3575 │    0.00 │ 0.000000 │ WILLIAM JOHN ERNEST  PARR… │ AKZO NOBEL           │ C09K**F17D**B01F**C23F     │ # noqa E501
    # │      3779 │   52.35 │ 4.916667 │ GUENTER  KOCHSMEIER**ZBIG… │ ALCATEL              │ G02B**G04G**H02G**G06F     │ # noqa E501
    # │      3780 │   52.35 │ 4.916667 │ ZILAN  MANFRED**JOSIANE  … │ ALCATEL              │ H03G**B05D**H04L**H04B**C… │ # noqa E501
    # │      3782 │    0.00 │ 0.000000 │ OLIVIER  AUDOUIN**MICHEL … │ ALCATEL              │ H04B**H01S**H04J           │ # noqa E501
    # │     15041 │    0.00 │ 0.000000 │ LEE  RICKLER**SIMON  PARK… │ CANON EUROPA         │ G06F                       │ # noqa E501
    # │     15042 │    0.00 │ 0.000000 │ QI HE  HONG**ADAM MICHAEL… │ CANON EUROPA         │ G06T**G01B                 │ # noqa E501
    # │     15043 │    0.00 │ 0.000000 │ NILESH  PATHAK**MASAMICHI… │ CANON EUROPA         │ H04B**G06T**G06F**H04M**H… │ # noqa E501
    # │     25387 │    0.00 │ 0.000000 │ GABRIEL MARINUS  MEESTERS… │ DSM                  │ C12N**A61K**A23L**A23J**A… │ # noqa E501
    # │         … │       … │        … │ …                          │ …                    │ …                          │ # noqa E501
    # └───────────┴─────────┴──────────┴────────────────────────────┴──────────────────────┴────────────────────────────┘ # noqa E501

    # labels looks like
    # |      |   person_id |   leuven_id | person_name                   |
    # |-----:|------------:|------------:|:------------------------------|
    # |    0 |        2909 |      402600 | * AGILENT TECHNOLOGIES, INC.  |
    # |    1 |        3574 |      569309 | * AKZO NOBEL N.V.             |
    # |    2 |        3575 |      569309 | * AKZO NOBEL NV               |
    # |    3 |        3779 |      656303 | * ALCATEL N.V.
    # It's the same length as df, where each row of labels corresponds to the
    # same row of df.
    t = t.inner_join(labels, "person_id")
    t = t.select(
        record_id=_.person_id,
        label_true=_.leuven_id,
        name_true=_.Name,
        name=_.person_name,
        latitude=_.Lat,
        longitude=_.Lng,
        coauthors=_.Coauthor,
        classes=_.Class,
    )
    t = t.order_by("record_id")
    t = t.cache()
    return t
