from __future__ import annotations

from typing import Callable, Iterable, Literal

import ibis
from ibis import _
from ibis.expr.types import Column, IntegerColumn, Table


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


def sample_table(
    table: Table,
    n_approx: int,
    *,
    method: Literal["row", "block"] | None = None,
    seed: int | None = None,
) -> Table:
    """Sample a table to approximately n rows.

    Due to the way ibis does sampling, this is not guaranteed to be exactly n rows.
    This is a thin wrapper around
    [ibis's sample method](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.sample)
    , which is a wrapper
    around the SAMPLE functionality in SQL. It is important to understand the
    behaviors and limitations of `seed`, the different sampling methods,
    and how they relate to `n_approx`.

    Parameters
    ----------
    table
        The table to sample
    n_approx
        The approximate number of rows to sample
    method
        The sampling method to use. If None, use "row" for small tables and "block" for
        large tables.

        See Ibis's documentation for more details:

    seed
        The random seed to use for sampling. If None, use a random seed.
    """
    if method is None:
        method = "row" if n_approx <= 2048 * 8 else "block"
    table = table.cache()
    n_available = table.count().execute()
    fraction = n_approx / n_available
    return table.sample(fraction, method=method, seed=seed)


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


def group_id(keys: str | Column | Iterable[str | Column]) -> IntegerColumn:
    """Number each group from 0 to "number of groups - 1".

    This is equivalent to pandas.DataFrame.groupby(keys).ngroup().
    """
    return ibis.dense_rank().over(ibis.window(order_by=keys)).cast("uint64")


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
    augmented = t.mutate(group_id(column).name(int_col_name))
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
