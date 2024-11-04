from __future__ import annotations

from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Literal, Mapping, TypeVar
import warnings

import ibis
from ibis import _
from ibis.common.deferred import Deferred
from ibis.expr import datatypes as dt
from ibis.expr import types as ir


def cases(
    *case_result_pairs: tuple[ir.BooleanValue, ir.Value],
    else_: ir.Value | None = None,
) -> ir.Value:
    """A more concise way to write a case statement."""
    builder = ibis.case()
    for case, result in case_result_pairs:
        builder = builder.when(case, result)
    return builder.else_(else_).end()


def bind(t: ir.Table, ref: Any) -> tuple[ir.Value]:
    """Reference into a table to get Columns and Scalars."""
    # ibis's bind() can't handle bind(_, "column_name")
    if isinstance(ref, str):
        return (t[ref],)
    return t.bind(ref)


def get_column(
    t: ir.Table, ref: Any, *, on_many: Literal["error", "struct"] = "error"
) -> ir.Column:
    """Get a column from a table using some sort of reference to the column.

    ref can be a string, a Deferred, a callable, an ibis selector, etc.

    Parameters
    ----------
    t :
        The table
    ref :
        The reference to the column
    on_many :
        What to do if ref returns multiple columns. If "error", raise an error.
        If "struct", return a StructColumn containing all the columns.
    """
    cols = bind(t, ref)
    if len(cols) != 1:
        if on_many == "error":
            raise ValueError(f"Expected 1 column, got {len(cols)}")
        if on_many == "struct":
            return ibis.struct({c.get_name(): c for c in cols})
        raise ValueError(f"on_many must be 'error' or 'struct'. Got {on_many}")
    return cols[0]


def ensure_ibis(
    val: Any, type: str | dt.DataType | None = None
) -> ir.Value | ibis.Deferred:
    """Ensure that `val` is an ibis expression."""
    if isinstance(val, ir.Expr) or isinstance(val, ibis.Deferred):
        return val
    return ibis.literal(val, type=type)


def get_name(x) -> str:
    """Find a suitable string representation of `x` to use as a blocker name."""
    if isinstance(x, Deferred):
        return x.__repr__()
    try:
        n = x.name
        if isinstance(n, str):
            return n
    except AttributeError:
        pass
    try:
        return x.get_name()
    except AttributeError:
        pass
    if isinstance(x, tuple):
        return "(" + ", ".join(get_name(y) for y in x) + ")"
    if is_iterable(x):
        return "[" + ", ".join(get_name(y) for y in x) + "]"
    return str(x)


def sample_table(
    table: ir.Table,
    n_approx: int,
    *,
    method: Literal["row", "block"] | None = None,
    seed: int | None = None,
) -> ir.Table:
    """Sample a table to approximately n rows.

    Due to the way ibis does sampling, this is not guaranteed to be exactly n rows.
    This is a thin wrapper around
    [ibis's sample method](https://ibis-project.org/reference/expression-tables#ibis.expr.types.relations.Table.sample)
    , which is a wrapper
    around the SAMPLE functionality in SQL. It is important to understand the
    behaviors and limitations of `seed`, the different sampling methods,
    and how they relate to `n_approx`.

    In addition, using `seed` does not guarantee reproducibility if the input
    table is not deterministic, which can happen unexpectedly. For example,
    if you use `.read_csv()` with duckdb, by default it uses a parallel CSV
    reader, which does not guarantee row order. So if you pass that table into
    this function, you could get different results each time. To fix this,
    you can pass `parallel=False` to `.read_csv()`, or use `.order_by()`.

    Parameters
    ----------
    table
        The table to sample
    n_approx
        The approximate number of rows to sample
    method
        The sampling method to use. If None, use "row" for small tables and "block" for
        large tables.

        See [Ibis's documentation on .sample()](https://ibis-project.org/reference/expression-tables.html#ibis.expr.types.relations.Table.sample)
        for more details.
    seed
        The random seed to use for sampling. If None, use a random seed.
    """
    if method is None:
        method = "row" if n_approx <= 2048 * 8 else "block"
    n_available = table.count().execute()
    fraction = n_approx / n_available
    return table.sample(fraction, method=method, seed=seed)


def group_id(
    keys: str | ir.Column | Iterable[str | ir.Column], *, dtype="!uint64"
) -> ir.IntegerColumn:
    """Number each group from 0 to "number of groups - 1".

    This is equivalent to pandas.DataFrame.groupby(keys).ngroup().
    The returned values is of type `dtype`.
    NULLs in the input are labeled just like any other value.
    """
    return ibis.dense_rank().over(ibis.window(order_by=keys)).cast(dtype)


_i = 0


def unique_name(prefix: str | None = None) -> str:
    """Find a universally unique name"""
    global _i
    if prefix is None:
        prefix = "__temp_"
    return f"{prefix}{_i}"


def unique_column_name(t: ir.Table) -> str:
    """Return a column name that is not already in the table"""
    i = 0
    while True:
        name = f"__unique_column_name_{i}__"
        if name not in t.columns:
            return name
        i += 1


def intify_column(
    t: ir.Table, column: str
) -> tuple[ir.Table, Callable[[ir.Table], ir.Table]]:
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

    def restore(with_int_labels: ir.Table) -> ir.Table:
        return ibis.join(
            with_int_labels,
            mapping,
            with_int_labels[column] == mapping[int_col_name],
            how="left",
            lname="{name}_int",
            rname="",
        ).drop(column + "_int", int_col_name)

    return augmented, restore


@contextmanager
def optional_import(pip_name: str):
    """
    Raises a more helpful ImportError when an optional dep is missing.

    with optional_import():
        import some_optional_dep
    """
    try:
        yield
    except ImportError as e:
        raise ImportError(
            f"Package `{pip_name}` is required for this functionality. "
            f"Please install it separately using `python -m pip install {pip_name}`."
        ) from e


V = TypeVar("V")


def promote_list(val: V | Sequence[V]) -> list[V]:
    """Ensure that the value is a list.

    Parameters
    ----------
    val
        Value to promote

    Returns
    -------
    list
    """
    if isinstance(val, list):
        return val
    elif isinstance(val, dict):
        return [val]
    elif is_iterable(val):
        return list(val)
    elif val is None:
        return []
    else:
        return [val]


def is_iterable(o: Any) -> bool:
    """Return whether `o` is iterable and not a :class:`str` or :class:`bytes`.

    Parameters
    ----------
    o : object
        Any python object

    Returns
    -------
    bool

    Examples
    --------
    >>> is_iterable("1")
    False
    >>> is_iterable(b"1")
    False
    >>> is_iterable(iter("1"))
    True
    >>> is_iterable(i for i in range(1))
    True
    >>> is_iterable(1)
    False
    >>> is_iterable([])
    True
    """
    if isinstance(o, (str, bytes)):
        return False

    try:
        iter(o)
    except TypeError:
        return False
    else:
        return True


def struct_equal(
    left: ir.StructValue, right: ir.StructValue, *, fields: Iterable[str] | None = None
) -> ir.BooleanValue:
    """
    The specified fields match exactly. If fields is None, all fields are compared.
    """
    if fields is None:
        return left == right
    return ibis.and_(*(left[f] == right[f] for f in fields))


def struct_isnull(
    struct: ir.StructValue, *, how: Literal["any", "all"], fields: Iterable[str] | None
) -> ir.BooleanValue:
    """Are any/all of the specified fields null (or the struct itself is null)?

    If fields is None, all fields are compared."""
    if fields is None:
        fields = struct.type().names
    vals = [struct[f].isnull() for f in fields]
    if how == "any":
        return struct.isnull() | ibis.or_(*vals)
    elif how == "all":
        return struct.isnull() | ibis.and_(*vals)
    else:
        raise ValueError(f"how must be 'any' or 'all'. Got {how}")


def struct_join(struct: ir.StructValue, sep: str) -> ir.StringValue:
    """Join all the fields in a struct with a separator."""
    regular = ibis.literal(sep).join(
        [struct[f].fill_null("") for f in struct.type().names]
    )
    return struct.isnull().ifelse(None, regular)


def struct_tokens(struct: ir.StructValue, *, unique: bool = True) -> ir.ArrayValue:
    """Get all the tokens from a struct."""
    oneline = struct_join(struct, " ")
    tokens = oneline.re_split(r"\s+")
    tokens = tokens.filter(lambda x: x != "")
    if unique:
        tokens = tokens.unique()
    return tokens


def join_lookup(
    t: ibis.Table,
    lookup: ibis.Table,
    on: str,
    *,
    defaults: Mapping[str, ir.Expr] | Iterable[ir.Expr] | None = None,
    augment_as: str | Callable[[str], str] | Literal[False] = "{name}",
    on_collision: Literal["error", "warn", "ignore"] = "error",
) -> ibis.Table:
    """Join a lookup table to the input table.

    Sometimes, there for a row in `t` there is no corresponding row in the lookup table.
    In that case, you can provide a `defaults` to provide default values that
    will get added to the lookup table.

    Parameters
    ----------
    t:
        Table to join with the lookup table.
    lookup:
        Table with one row per group.
    on:
        The column that will be used to join the two tables.
        The lookup table should have one row per group.
        The table `t` can have multiple rows per group.
        For each row in `t`, we will look up the corresponding row in the lookup table.
    defaults:
        If `lookup` doesn't contain a row for a group in `t`,
        add a row with these defaults.
        For any fields not given, we will fill in with NULL.
    augment_as:
        How to rename the columns in the lookup table before joining.
        If a string, use that as a format string to rename the columns.
        If a callable, use the callable to rename the columns.
        If False, then *we don't actually do any joining*,
        and just simply return the lookup table directly, after adding in any defaults.

        For example, if `rename_as` is "{name}_canonical",
        then the lookup table's "email" column will be renamed to "email_canonical"
        before being joined to the input table.
        If you want to overwrite the columns in the input table with the lookup table,
        you can use `rename_as="{name}"`.

        You could also pass `lambda col: col.upper()` to make all the
        added columns uppercase.
    on_collision:
        What to do if there are columns in `t` that will be overwritten by the join.
        If `rename_as` is the special value "{name}", then this parameter is ignored,
        since it is assumed you want to overwrite the columns.
    """
    if defaults is None:
        defaults = {col: ibis.null() for col in lookup.columns if col != on}
    if not isinstance(defaults, Mapping):
        defaults = {col.get_name(): col for col in defaults}
    # Ensure that the types are mergable. eg we want to support passing in
    # {"emails": ibis.literal([])} as a default (which has no concrete type)
    # First ensure all are expressions:
    defaults = {
        col: val if isinstance(val, ir.Expr) else ibis.literal(val)
        for col, val in defaults.items()
    }
    defaults = {col: val.cast(lookup[col].type()) for col, val in defaults.items()}

    lookup = ibis.union(
        lookup,
        t.select(on, **defaults).distinct().anti_join(lookup, on),
    )

    if augment_as is False:
        return lookup

    def rename(col: str) -> str:
        if col == on:
            return col
        if isinstance(augment_as, str):
            return augment_as.format(name=col)
        return augment_as(col)

    lookup = lookup.rename(rename)
    collisions = (set(t.columns) & set(lookup.columns)) - {on}
    _check_collisions(collisions, on_collision, augment_as, t.columns)
    t = t.drop(*collisions)
    return t.left_join(lookup, on).drop(on + "_right")


def _check_collisions(collisions, on_collision, rename_as, columns):
    msg = f"Columns {collisions} will be overwritten in table with {columns}"

    def _err():
        raise ValueError(msg)

    def _warn():
        warnings.warn(msg)

    if on_collision == "error":
        f = _err
    elif on_collision == "warn":
        f = _warn
    elif on_collision == "ignore":
        f = lambda: None  # noqa: E731
    else:
        raise ValueError(f"Unknown on_collision: {on_collision}")
    if collisions and rename_as != "{name}":
        f()
