from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import types as ir


def term_frequency_table(terms: ir.ArrayColumn) -> ir.Table:
    """Compute a lookup table for the document frequency of terms.

    Based around https://en.wikipedia.org/wiki/Tf%E2%80%93idf.

    Parameters
    ----------
        terms: Column of Arrays holding terms, one array per record.

    Returns
    -------
        A Table with a single row for every unique term in the input, with columns:
        - term: the term
        - n_records: the number of records containing the term
        - frac_records: the fraction of records containing the term
        - idf: the inverse document frequency of the term
            (0 means the term is in every record, large numbers mean the term is rare)

    Examples
    --------
    >>> import ibis
    >>> from mismo import term_frequency_table
    ... >>> addresses = [
    ...     "12 main st",
    ...     "34 st john ave",
    ...     "56 oak st",
    ...     "12 glacier st",
    ... ]
    >>> t = ibis.memtable({"address": addresses})
    >>> # split on whitespace
    >>> tokens = t.address.re_split(r"\s+")
    >>> idf_table(tokens)
    ┏━━━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━┓
    ┃ term    ┃ n_records ┃ frac_records ┃ idf      ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━┩
    │ string  │ int64     │ float64      │ float64  │
    ├─────────┼───────────┼──────────────┼──────────┤
    │ 12      │         2 │         0.50 │ 0.693147 │
    │ 56      │         1 │         0.25 │ 1.386294 │
    │ john    │         1 │         0.25 │ 1.386294 │
    │ 34      │         1 │         0.25 │ 1.386294 │
    │ main    │         1 │         0.25 │ 1.386294 │
    │ oak     │         1 │         0.25 │ 1.386294 │
    │ glacier │         1 │         0.25 │ 1.386294 │
    │ st      │         4 │         1.00 │ 0.000000 │
    │ ave     │         1 │         0.25 │ 1.386294 │
    └─────────┴───────────┴──────────────┴──────────┘
    """
    if not isinstance(terms, ir.ArrayValue):
        raise ValueError(f"Unsupported type {type(terms)}")
    terms_table = (
        terms.name("terms")
        .as_table()
        .select(
            record_id=ibis.row_number(),
            terms=_.terms,
        )
    )
    n_total_records = terms_table.view().record_id.nunique()
    flat = terms_table.select("record_id", term=_.terms.unnest())
    by_term = flat.group_by("term").agg(n_records=_.record_id.nunique())
    by_term = by_term.select(
        "term",
        "n_records",
        frac_records=by_term.n_records / n_total_records,
        idf=(n_total_records / by_term.n_records).log(),
    )
    return by_term
