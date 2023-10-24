from __future__ import annotations

import math
import random
from typing import Callable, Iterable

import ibis
from ibis import _
from ibis.expr.types import Column, IntegerColumn, Table
import numpy as np


def format_table(template: str, name: str, table: Table) -> str:
    t = repr(table.head(5))
    nindent = 0
    search = "{" + name + "}"
    for line in template.splitlines():
        try:
            nindent = line.index(search)
        except ValueError:
            continue
    indent = " " * nindent
    sep = "\n" + indent
    t = sep.join(line for line in t.splitlines())
    return template.format(table=t)


# TODO: This would be great if it were actually part of Ibis.
# This shouldn't ever be so big it runs us out of memory, but still.
def sample_table(table: Table, n: int = 5, seed: int | None = None) -> Table:
    if seed is not None:
        random.seed(seed)
    n_available = table.count().execute()
    n_repeats = math.ceil(n / n_available)
    pool = np.repeat(np.arange(n_available), n_repeats)
    idx = np.random.choice(pool, size=n, replace=False)
    idx_table = ibis.memtable({"__idx__": idx})
    table = table.mutate(__idx__=ibis.row_number())
    return table.inner_join(idx_table, "__idx__").drop("__idx__")  # type: ignore


def join(
    left: Table,
    right: Table,
    predicates=tuple(),
    how="inner",
    *,
    lname: str = "",
    rname: str = "{name}_right",
) -> Table:
    """Similar to ibis's join, with a few differences

    - Does a cross join when predicates is True or how is "cross"
    - Converts the lname and rname suffixes to the appropriate kwargs for ibis<6.0.0
    - Allows for a wider set of join predicates:
      - anything that ibis accepts as a join predicate
      - tuple[str, str]
      - tuple[Column, Column]
      - tuple[Deferred, Deferred]
      - lambda (left, right) -> any of the above
    """
    rename_kwargs = _join_suffix_kwargs(lname=lname, rname=rname)
    preds = _to_ibis_join_predicates(left, right, predicates)
    return left.join(right, predicates=preds, how=how, **rename_kwargs)


def _to_ibis_join_predicates(left, right, raw_predicates) -> tuple:
    if isinstance(raw_predicates, tuple):
        if len(raw_predicates) != 2:
            raise ValueError(
                f"predicates must be a tuple of length 2, got {raw_predicates}"
            )
        # Ibis has us covered with one adjustment
        # https://github.com/ibis-project/ibis/pull/7424
        return [raw_predicates]
    if callable(raw_predicates):
        return _to_ibis_join_predicates(left, right, raw_predicates(left, right))
    else:
        return raw_predicates


def _join_suffix_kwargs(lname: str, rname: str) -> dict:
    """create the suffix kwargs for ibis.join(), no matter the ibis version.

    The suffixes kwarg got split into lname and rname in ibis 6.0.0:
    https://ibis-project.org/release_notes/#600-2023-07-05"""
    if ibis.__version__ >= "6.0.0":
        return {"lname": lname, "rname": rname}
    else:

        def _convert_suffix(suffix: str) -> str:
            if not len(suffix):
                return ""
            if not suffix.startswith("{name}"):
                raise ValueError(
                    "suffix must be empty or start with '{name}'"
                    f"for ibis<6.0.0, got {suffix}"
                )
            return suffix.removeprefix("{name}")

        lsuffix = _convert_suffix(lname)
        rsuffix = _convert_suffix(rname)
        return {"suffixes": (lsuffix, rsuffix)}


def group_id(t: Table, keys: str | Column | Iterable[str | Column]) -> IntegerColumn:
    """Number each group from 0 to "number of groups - 1".

    This is equivalent to pandas.DataFrame.groupby(keys).ngroup().
    """
    # We need an arbitrary column to use for dense_rank
    # https://github.com/ibis-project/ibis/issues/5408
    col: Column = t[t.columns[0]]
    return col.dense_rank().over(ibis.window(order_by=keys)).cast("uint64")


def unique_column_name(t: Table) -> str:
    """Return a column name that is not already in the table"""
    i = 0
    while True:
        name = f"__unique_column_name_{i}__"
        if name not in t.columns:
            return name
        i += 1


def intify_column(t: Table, column: str) -> tuple[Table, Callable[[Table], Table]]:
    """
    Translate column to integer, and return a restoring function.

    This is useful if you want to use integer codes to do some operation, but
    then want to restore the original column.
    """
    if t[column].type().is_integer():
        return t, lambda x: x

    int_col_name = unique_column_name(t)
    augmented = t.mutate(group_id(t, column).name(int_col_name))
    mapping = augmented.select(int_col_name, column).distinct()
    augmented = augmented.mutate(**{column: _[int_col_name]}).drop(int_col_name)

    def restore(with_int_labels: Table) -> Table:
        return join(
            with_int_labels,
            mapping,
            with_int_labels[column] == mapping[int_col_name],
            how="left",
            lname="{name}_int",
            rname="",
        ).drop(column + "_int", int_col_name)

    return augmented, restore
