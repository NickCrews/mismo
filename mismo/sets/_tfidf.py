from __future__ import annotations

import ibis
from ibis import _
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _util, vector


def document_counts(terms: ir.ArrayColumn) -> ir.Table:
    r"""Create a lookup Table from term to number of records containing the term.

    Parameters
    ----------
    terms:
        One row for each record. Each row is an array of terms in that record.
        Each term could be a word, ngram, or other token from a string.
        Or, it could also represent more generic data, such as a list of tags or
        categories like ["red", "black"]. Each term can be any datatype,
        not just strings.

    Returns
    -------
        A Table with columns `term` and `n_records`. The `term` column contains
        each unique term from the input `terms` array. The `n_records` column
        contains the number of records in the input `terms` array that contain

    Examples
    --------
    >>> import ibis
    >>> ibis.options.interactive = True
    >>> from mismo.sets import document_counts
    >>> addresses = [
    ...     "12 main st",
    ...     "99 main ave",
    ...     "56 st joseph st",
    ...     "21 glacier st",
    ...     "12 glacier st",
    ... ]
    >>> t = ibis.memtable({"address": addresses})
    >>> # split on whitespace
    >>> t = t.mutate(terms=t.address.re_split(r"\s+"))
    >>> document_counts(t.terms).order_by("term")
    ┏━━━━━━━━━┳━━━━━━━━━━━┓
    ┃ term    ┃ n_records ┃
    ┡━━━━━━━━━╇━━━━━━━━━━━┩
    │ string  │ int64     │
    ├─────────┼───────────┤
    │ 12      │         2 │
    │ 21      │         1 │
    │ 56      │         1 │
    │ 99      │         1 │
    │ ave     │         1 │
    │ glacier │         2 │
    │ joseph  │         1 │
    │ main    │         2 │
    │ st      │         4 │
    └─────────┴───────────┘
    """  # noqa: E501
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
    flat = terms_table.select("record_id", term=_.terms.unnest())
    by_term = flat.group_by("term").agg(n_records=_.record_id.nunique())
    return by_term


def term_idf(terms: ir.ArrayValue) -> ir.Table:
    r"""Create a lookup Table from term to IDF.

    Examples
    --------
    >>> import ibis
    >>> from mismo.sets import term_idf
    >>> ibis.options.interactive = True
    >>> addresses = [
    ...     "12 main st",
    ...     "99 main ave",
    ...     "56 st joseph st",
    ...     "21 glacier st",
    ...     "12 glacier st",
    ... ]
    >>> t = ibis.memtable({"address": addresses})
    >>> # split on whitespace
    >>> t = t.mutate(terms=t.address.re_split(r"\s+"))
    >>> term_idf(t.terms).order_by("term")
    ┏━━━━━━━━━┳━━━━━━━━━━┓
    ┃ term    ┃ idf      ┃
    ┡━━━━━━━━━╇━━━━━━━━━━┩
    │ string  │ float64  │
    ├─────────┼──────────┤
    │ 12      │ 0.916291 │
    │ 21      │ 1.609438 │
    │ 56      │ 1.609438 │
    │ 99      │ 1.609438 │
    │ ave     │ 1.609438 │
    │ glacier │ 0.916291 │
    │ joseph  │ 1.609438 │
    │ main    │ 0.916291 │
    │ st      │ 0.223144 │
    └─────────┴──────────┘
    """
    dc = document_counts(terms)
    n_total_records = terms.count()
    idf = dc.select(
        "term",
        idf=(n_total_records / dc.n_records).log(),
    )
    return idf


# TODO: if https://github.com/ibis-project/ibis/issues/8614
# is fixed, this API can improve to be (ArrayValue) -> MapValue
def add_array_value_counts(
    t: ir.Table, column: str, *, result_name: str = "{name}_counts"
) -> ir.Table:
    r"""value_counts() for ArrayColumns.

    Parameters
    ----------
    t : Table
        The input table.
    column : str
        The name of the array column to analyze.
    result_name : str, optional
        The name of the resulting column. The default is "{name}_counts".

    Examples
    --------
    >>> import ibis
    >>> from mismo.sets import add_array_value_counts
    >>> ibis.options.interactive = True
    >>> ibis.options.repr.interactive.max_length = 20
    >>> terms = [
    ...     None,
    ...     ["st"],
    ...     ["st"],
    ...     ["12", "main", "st"],
    ...     ["99", "main", "ave"],
    ...     ["56", "st", "joseph", "st"],
    ...     ["21", "glacier", "st"],
    ...     ["12", "glacier", "st"],
    ... ]
    >>> t = ibis.memtable({"terms": terms})
    >>> add_array_value_counts(t, "terms")  # doctest: +SKIP
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ terms                        ┃ terms_counts                     ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                │ map<string, int64>               │
    ├──────────────────────────────┼──────────────────────────────────┤
    │ ['st']                       │ {'st': 1}                        │
    │ ['st']                       │ {'st': 1}                        │
    │ ['12', 'main', 'st']         │ {'st': 1, '12': 1, 'main': 1}    │
    │ ['99', 'main', 'ave']        │ {'ave': 1, '99': 1, 'main': 1}   │
    │ ['56', 'st', 'joseph', 'st'] │ {'56': 1, 'joseph': 1, 'st': 2}  │
    │ ['21', 'glacier', 'st']      │ {'glacier': 1, 'st': 1, '21': 1} │
    │ ['12', 'glacier', 'st']      │ {'glacier': 1, 'st': 1, '12': 1} │
    │ NULL                         │ NULL                             │
    └──────────────────────────────┴──────────────────────────────────┘
    """  # noqa: E501
    t = t.mutate(__terms=_util.get_column(t, column))
    normalized = (
        t.select("__terms")
        .distinct()
        .mutate(
            __term=_.__terms.filter(lambda t: t.notnull()).unnest(),
        )
    )
    counts = normalized.group_by(["__terms", "__term"]).agg(
        __n=_.count(),
    )
    by_terms = counts.group_by("__terms").agg(
        __result=ibis.map(_.__term.collect(), _.__n.collect()),
    )
    result = t.left_join(by_terms, "__terms").drop("__terms", "__terms_right")

    # annoying logic to deal with the edge case of an empty array
    term_type = t.__terms.type().value_type
    counts_type = dt.Map(key_type=term_type, value_type=dt.int64)
    empty = ibis.literal({}, counts_type)
    result = result.mutate(__result=(_[column].length() == 0).ifelse(empty, _.__result))
    return result.rename({result_name.format(name=column): "__result"})


def add_tfidf(
    t,
    column: str,
    *,
    result_name: str = "{name}_tfidf",
    normalize: bool = True,
):
    r"""Vectorize terms using TF-IDF.

    Adds a column to the input table that contains the TF-IDF vector for the
    terms in the input column.

    Parameters
    ----------
    t : Table
        The input table.
    column : str
        The name of the array column to analyze.
    result_name : str, optional
        The name of the resulting column. The default is "{name}_tfidf".
    normalize :
        Whether to normalize the TF-vector before multiplying by the IDF.
        The default is True.
        This makes it so that vectors of different lengths can be compared fairly.

    Examples
    --------
    >>> import ibis
    >>> from mismo.sets import add_tfidf
    >>> ibis.options.interactive = True
    >>> ibis.options.repr.interactive.max_length = 20
    >>> terms = [
    ...     None,
    ...     ["st"],
    ...     ["st"],
    ...     ["12", "main", "st"],
    ...     ["99", "main", "ave"],
    ...     ["56", "st", "joseph", "st"],
    ...     ["21", "glacier", "st"],
    ...     ["12", "glacier", "st"],
    ... ]
    >>> t = ibis.memtable({"terms": terms})
    >>> add_tfidf(t, "terms")  # doctest: +SKIP
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ terms                        ┃ terms_tfidf                                                                          ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                │ map<string, float64>                                                                 │
    ├──────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────┤
    │ ['st']                       │ {'st': 0.15415067982725836}                                                          │
    │ ['st']                       │ {'st': 0.15415067982725836}                                                          │
    │ ['12', 'glacier', 'st']      │ {'12': 0.7232830370915955, 'glacier': 0.7232830370915955, 'st': 0.08899893649403144} │
    │ ['12', 'main', 'st']         │ {'12': 0.7232830370915955, 'main': 0.7232830370915955, 'st': 0.08899893649403144}    │
    │ ['21', 'glacier', 'st']      │ {'21': 1.12347174837591, 'glacier': 0.7232830370915955, 'st': 0.08899893649403144}   │
    │ ['56', 'st', 'joseph', 'st'] │ {'56': 0.7944144917481126, 'joseph': 0.7944144917481126, 'st': 0.12586350302664107}  │
    │ ['99', 'main', 'ave']        │ {'main': 0.7232830370915955, 'ave': 1.12347174837591, '99': 1.12347174837591}        │
    │ NULL                         │ NULL                                                                                 │
    └──────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────┘
    """  # noqa: E501
    with_counts = add_array_value_counts(t, column, result_name="__term_counts")
    if normalize:
        with_counts = with_counts.mutate(
            __term_counts=vector.normalize(with_counts.__term_counts)
        )
    idf = term_idf(t[column])
    with_counts = with_counts.mutate(__row_number=ibis.row_number())
    term_counts = with_counts.select("__row_number", "__term_counts")
    term_counts = term_counts.mutate(
        term=_.__term_counts.keys().unnest(),
        term_count=_.__term_counts.values().unnest(),
    ).drop("__term_counts")
    idf_table = term_counts.join(idf, "term")
    idf_table = idf_table.mutate(idf=_.idf * _.term_count).drop("term_count")
    r = result_name.format(name=column)
    idf_table = idf_table.group_by("__row_number").aggregate(
        ibis.map(idf_table.term.collect(), idf_table.idf.collect()).name(r)
    )
    result = with_counts.left_join(idf_table, "__row_number").drop(
        "__row_number", "__term_counts", "__row_number_right"
    )
    empty = ibis.literal({}, type=result[r].type())
    result = result.mutate((_[column].length() == 0).ifelse(empty, _[r]).name(r))
    return result


def rare_terms(
    terms: ir.ArrayColumn,
    *,
    max_records_n: int | None = None,
    max_records_frac: float | None = None,
) -> ir.Column:
    """Get the terms that appear in few records.

    The returned Column is flattened. Eg if you supply a column of `array<string>`,
    the result will be of type `string`.

    Exactly one of `max_records_n` or `max_records_frac` must be set.

    Parameters
    ----------
    terms : ArrayColumn
        A column of Arrays, where each array contains the terms for a record.
    max_records_n : int, optional
        The maximum number of records a term can appear in. The default is None.
    max_records_frac : float, optional
        The maximum fraction of records a term can appear in. The default is None.

    Returns
    -------
    Column
        The terms that appear in few records.
    """
    if max_records_n is not None and max_records_frac is not None:
        raise ValueError("Only one of max_records_n or max_records_frac can be set")
    if max_records_n is None and max_records_frac is None:
        raise ValueError("One of max_records_n or max_records_frac must be set")
    dc = document_counts(terms)
    if max_records_n is not None:
        rare = dc.filter(_.n_records <= max_records_n)
    else:
        n_total_records = terms.count()
        dc = dc.mutate(frac=_.n_records / n_total_records)
        rare = dc.filter(_.frac <= max_records_frac)
    return rare["term"]
