from __future__ import annotations

from pathlib import Path

import ibis
from ibis.expr import types as ir

from mismo.joins._conditions import left, right
from mismo.linkage._linkage import Linkage

__all__ = [
    "load_febrl1",
    "load_febrl2",
    "load_febrl3",
    "load_patents",
    "load_rldata500",
    "load_rldata10000",
]

_DATASETS_DIR = Path(__file__).parent / "_data/_datasets"


def _febrl_load_data(
    dataset_num: int, backend: ibis.BaseBackend | None = None
) -> Linkage:
    """Internal function for loading FEBRL data from CSV files."""
    if backend is None:
        backend = ibis

    filepath = _DATASETS_DIR / "febrl" / f"dataset{dataset_num}.csv"

    # Read CSV with proper dtypes to match the original behavior
    schema = {
        "record_id": "uint16",
        "label_true": "uint16",
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
    records = backend.read_csv(filepath)
    records = records.cast(schema)
    records = records.cache()
    return _linkage_from_labels(records)


def _linkage_from_labels(records: ir.Table) -> Linkage:
    """Create a Linkage object from left and right tables using label_true."""
    condition = ibis.and_(
        left.label_true == right.label_true,
        left.record_id < right.record_id,
    )
    return Linkage.from_join_condition(
        left=records, right=records.view(), condition=condition
    )


def load_febrl1(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Load the FEBRL 1 dataset.

    The Freely Extensible Biomedical Record Linkage (Febrl) package is distributed
    with a dataset generator and four datasets generated with the generator.
    """
    return _febrl_load_data(1, backend=backend)


def load_febrl2(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Load the FEBRL 2 dataset.

    The Freely Extensible Biomedical Record Linkage (Febrl) package is distributed
    with a dataset generator and four datasets generated with the generator.
    """
    return _febrl_load_data(2, backend=backend)


def load_febrl3(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Load the FEBRL 3 dataset.

    The Freely Extensible Biomedical Record Linkage (Febrl) package is distributed
    with a dataset generator and four datasets generated with the generator.
    """
    return _febrl_load_data(3, backend=backend)


# Don't bother wrapping load_febrl4 because it has a different API,
# could add that later if it's needed.


def load_patents(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Load the PATSTAT dataset.

    This represents a dataset of patents, and the task is to determine which
    patents came from the same inventor.

    This comes from
    [the Dedupe Patent Example](https://github.com/dedupeio/dedupe-examples/tree/master/patent_example).

    Returns
    -------
    Linkage
        A [Linkage](mismo.Linkage), where both `left` and `right` are the tables
        of records. Each one has the following schema:

        - record_id: uint32
          A unique ID for each row in the table.
        - label_true: uint32
          The manually labeled, true ID of the inventor.
        - name_true: str
          The manually labeled, true name of the inventor.
        - name: str
          The raw name on the patent.
        - latitude: float64
          Geocoded from the inventor's address. 0.0 indicates no address was found
        - longitude: float64
        - coauthor: str
          A list of coauthors on the patent, separated by "**"
        - classes: str
          A list of 4-character IPC technical codes, separated by "**"

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> load_patents().left.head()  # doctest: +SKIP
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id ┃ label_true ┃ name_true            ┃ name                                             ┃ latitude ┃ longitude ┃ coauthors                                       ┃ classes                                         ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ uint32    │ uint32     │ string               │ string                                           │ float64  │ float64   │ string                                          │ string                                          │
    ├───────────┼────────────┼──────────────────────┼──────────────────────────────────────────────────┼──────────┼───────────┼─────────────────────────────────────────────────┼─────────────────────────────────────────────────┤
    │      2909 │     402600 │ AGILENT TECHNOLOGIES │ * AGILENT TECHNOLOGIES, INC.                     │     0.00 │  0.000000 │ KONINK PHILIPS ELECTRONICS N V**DAVID E  SNYDE… │ A61N**A61B                                      │
    │      3574 │     569309 │ AKZO NOBEL           │ * AKZO NOBEL N.V.                                │     0.00 │  0.000000 │ TSJERK  HOEKSTRA**ANDRESS K  JOHNSON**TERESA M… │ G01N**B01L**C11D**G02F**F16L                    │
    │      3575 │     569309 │ AKZO NOBEL           │ * AKZO NOBEL NV                                  │     0.00 │  0.000000 │ WILLIAM JOHN ERNEST  PARR**HANS  OSKARSSON**MA… │ C09K**F17D**B01F**C23F                          │
    │      3779 │     656303 │ ALCATEL              │ * ALCATEL N.V.                                   │    52.35 │  4.916667 │ GUENTER  KOCHSMEIER**ZBIGNIEW  WIEGOLASKI**EVA… │ G02B**G04G**H02G**G06F                          │
    │      3780 │     656303 │ ALCATEL              │ * ALCATEL N.V.                                   │    52.35 │  4.916667 │ ZILAN  MANFRED**JOSIANE  RAMOS**DUANE LYNN  MO… │ H03G**B05D**H04L**H04B**C03B**C03C**G02B**H01B  │
    └───────────┴────────────┴──────────────────────┴──────────────────────────────────────────────────┴──────────┴───────────┴─────────────────────────────────────────────────┴─────────────────────────────────────────────────┘
    """  # noqa E501
    if backend is None:
        backend = ibis
    # In order to guarantee row order, could either use
    # parallel=False kwarg, but I'd rather just have them sorted
    # by record_id

    schema = {
        "record_id": "uint32",
        "label_true": "uint32",
        "name_true": "string",
        "name": "string",
        "latitude": "float64",
        "longitude": "float64",
        "coauthors": "string",
        "classes": "string",
    }

    path = _DATASETS_DIR / "patstat/patents.csv"
    records = (
        backend.read_csv(path)
        .select(*schema.keys())
        .cast(schema)
        .order_by("record_id")
        .cache()
    )
    return _linkage_from_labels(records)


_RLDATA_SCHEMA = {
    "record_id": "int64",
    "label_true": "int64",
    "fname_c1": "string",
    "fname_c2": "string",
    "lname_c1": "string",
    "lname_c2": "string",
    "by": "int64",
    "bm": "int64",
    "bd": "int64",
}


def load_rldata500(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Synthetic personal information dataset with 500 rows

    This is a synthetic dataset with noisy names and dates of birth, with the task being
    to determine which rows represent the same person. 10% of the records are duplicates
    of existing ones, and the level of noise is low. The dataset can be deduplicated with 90%+
    precision and recall using simple linkage rules. It is often used as a
    sanity check for computational efficiency and disambiguation accuracy.

    This comes from the
    [RecordLinkage R package](https://cran.r-project.org/web/packages/RecordLinkage/index.html)
    and was generated using the data generation component of
    [Febrl (Freely Extensible Biomedical Record Linkage)](https://sourceforge.net/projects/febrl/).

    Returns
    -------
    Linkage
        A [Linkage](mismo.Linkage), where both `left` and `right` are the tables
        of records. Each one has the following schema:

        - record_id: int64
          A unique ID for each row in the table.
        - label_true: int64
          The manually labeled, true ID of the inventor.
        - fname_c1: string
          First component of the first name.
        - fname_c2: string
          Second component of the first name (mostly NULL values)
        - lname_c1: string
          First component of the last name.
        - lname_c2: string
          Second component of the last name (mostly NULL values).
        - by: int64
          Birth year
        - bm: int64
          Birth month
        - bd: int64
          Birth day

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> load_rldata500().left.head()  # doctest: +SKIP
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓
    ┃ record_id ┃ label_true ┃ fname_c1 ┃ fname_c2 ┃ lname_c1 ┃ lname_c2 ┃ by    ┃ bm    ┃ bd    ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩
    │ int64     │ int64      │ string   │ string   │ string   │ string   │ int64 │ int64 │ int64 │
    ├───────────┼────────────┼──────────┼──────────┼──────────┼──────────┼───────┼───────┼───────┤
    │         0 │         34 │ CARSTEN  │ NULL     │ MEIER    │ NULL     │  1949 │     7 │    22 │
    │         1 │         51 │ GERD     │ NULL     │ BAUER    │ NULL     │  1968 │     7 │    27 │
    │         2 │        115 │ ROBERT   │ NULL     │ HARTMANN │ NULL     │  1930 │     4 │    30 │
    │         3 │        189 │ STEFAN   │ NULL     │ WOLFF    │ NULL     │  1957 │     9 │     2 │
    │         4 │         72 │ RALF     │ NULL     │ KRUEGER  │ NULL     │  1966 │     1 │    13 │
    └───────────┴────────────┴──────────┴──────────┴──────────┴──────────┴───────┴───────┴───────┘
    """  # noqa: E501
    if backend is None:
        backend = ibis
    records = (
        backend.read_csv(_DATASETS_DIR / "rldata/RLdata500.csv")
        .select(*_RLDATA_SCHEMA.keys())
        .cast(_RLDATA_SCHEMA)
        .order_by("record_id")
        .cache()
    )
    return _linkage_from_labels(records)


def load_rldata10000(*, backend: ibis.BaseBackend | None = None) -> Linkage:
    """Synthetic personal information dataset with 10000 rows

    This is a synthetic dataset with noisy names and dates of birth, with the task being
    to determine which rows represent the same person. 10% of the records are duplicates
    of existing ones, and the level of noise is low. The dataset can be deduplicated with 90%+
    precision and recall using simple linkage rules. It is often used as a
    sanity check for computational efficiency and disambiguation accuracy.

    This comes from the
    [RecordLinkage R package](https://cran.r-project.org/web/packages/RecordLinkage/index.html)
    and was generated using the data generation component of
    [Febrl (Freely Extensible Biomedical Record Linkage)](https://sourceforge.net/projects/febrl/).

    Returns
    -------
    Linkage
        A [Linkage](mismo.Linkage), where both `left` and `right` are the tables
        of records. Each one has the following schema:

        - record_id: int64
          A unique ID for each row in the table.
        - label_true: int64
          The manually labeled, true ID of the inventor.
        - fname_c1: string
          First component of the first name.
        - fname_c2: string
          Second component of the first name (mostly NULL values)
        - lname_c1: string
          First component of the last name.
        - lname_c2: string
          Second component of the last name (mostly NULL values).
        - by: int64
          Birth year
        - bm: int64
          Birth month
        - bd: int64
          Birth day

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> load_rldata10000().left.head()  # doctest: +SKIP
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━┳━━━━━━━┳━━━━━━━┓
    ┃ record_id ┃ label_true ┃ fname_c1 ┃ fname_c2 ┃ lname_c1   ┃ lname_c2 ┃ by    ┃ bm    ┃ bd    ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━╇━━━━━━━╇━━━━━━━┩
    │ int64     │ int64      │ string   │ string   │ string     │ string   │ int64 │ int64 │ int64 │
    ├───────────┼────────────┼──────────┼──────────┼────────────┼──────────┼───────┼───────┼───────┤
    │         0 │       3606 │ FRANK    │ NULL     │ MUELLER    │ NULL     │  1967 │     9 │    27 │
    │         1 │       2560 │ MARTIN   │ NULL     │ SCHWARZ    │ NULL     │  1967 │     2 │    17 │
    │         2 │       3892 │ HERBERT  │ NULL     │ ZIMMERMANN │ NULL     │  1961 │    11 │     6 │
    │         3 │        329 │ HANS     │ NULL     │ SCHMITT    │ NULL     │  1945 │     8 │    14 │
    │         4 │       1994 │ UWE      │ NULL     │ KELLER     │ NULL     │  2000 │     7 │     5 │
    └───────────┴────────────┴──────────┴──────────┴────────────┴──────────┴───────┴───────┴───────┘
    """  # noqa: E501
    if backend is None:
        backend = ibis
    records = (
        backend.read_csv(_DATASETS_DIR / "rldata/RLdata10000.csv")
        .select(*_RLDATA_SCHEMA.keys())
        .cast(_RLDATA_SCHEMA)
        .order_by("record_id")
        .cache()
    )
    return _linkage_from_labels(records)
