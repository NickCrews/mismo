from __future__ import annotations

import math
import random

import ibis
from ibis.expr.types import Table
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

    - does a cross join when predicates is True or how is "cross"
    - converts the lname and rname suffixes to the appropriate kwargs for ibis<6.0.0
    """
    rename_kwargs = _join_suffix_kwargs(lname=lname, rname=rname)
    if how == "cross" or predicates is True:
        return left.cross_join(right, **rename_kwargs)
    else:
        return left.join(right, predicates=predicates, how=how, **rename_kwargs)


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
