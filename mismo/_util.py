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
