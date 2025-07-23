from __future__ import annotations

import base64
from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any, Callable, Iterable, Literal, Mapping, TypeVar
import uuid
import warnings

import ibis
from ibis import _
from ibis.common.deferred import Deferred
from ibis.expr import datatypes as dt
from ibis.expr import types as ir


class NotSet:
    def __repr__(self):
        return "NOT SET"

    def __str__(self):
        return "NOT SET"


NOT_SET = NotSet


def select(
    *values: ibis.Value | ibis.Table | Any, **named_values: ibis.Value | Any
) -> ibis.Table:
    """Like Table.select(), but you don't need to reference the table.

    Examples
    --------
    >>> ibis.options.interactive = True
    >>> t = ibis.memtable({"a": [1, 2, 3], "b": [4, 5, 6]})
    >>> select(t.a.name("aa"), b2=_.b * 2, x=3)
    ┏━━━━━━━┳━━━━━━━┳━━━━━━┓
    ┃ aa    ┃ b2    ┃ x    ┃
    ┡━━━━━━━╇━━━━━━━╇━━━━━━┩
    │ int64 │ int64 │ int8 │
    ├───────┼───────┼──────┤
    │     1 │     8 │    3 │
    │     2 │    10 │    3 │
    │     3 │    12 │    3 │
    └───────┴───────┴──────┘
    """

    def to_ibis_values(x):
        if isinstance(x, ibis.Value):
            return [x]
        if isinstance(x, ibis.Table):
            return [x[c] for c in x.columns]
        # literal values like plain old 5 or "my string" are ignored
        return []

    exprs = []
    for v in values:
        exprs.extend(to_ibis_values(v))
    for v in named_values.values():
        exprs.extend(to_ibis_values(v))
    parent_relations = {}
    for v in exprs:
        for r in v.op().relations:
            parent_relations[r] = parent_relations.get(r, set()) | {v.get_name()}
    if not parent_relations:
        raise ValueError("No parent relations found")
    if len(parent_relations) > 1:
        lines = [f"relation {r} owns values {vs}" for r, vs in parent_relations.items()]
        s = "\n  ".join(lines)
        raise ValueError("Multiple parent relations:\n" + s)
    parent = next(iter(parent_relations))
    return parent.to_expr().select(*values, **named_values)


def cases(
    *case_result_pairs: tuple[ir.BooleanValue, ir.Value],
    else_: ir.Value | None = None,
) -> ir.Value:
    """A more concise way to write a case statement."""
    try:
        # ibis.cases() was added in ibis 10.0.0
        cases = getattr(ibis, "cases")
        return cases(*case_result_pairs, else_=else_)
    except AttributeError:
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
    method: Literal["row", "block", "hash"] | None = None,
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

        If "hash", use a pseudorandom sample based on a hash of the rows.
        This requires hashing every row, and then sorting by the hash,
        so it can be slow, but it is guaranteed to be deterministic.
        See https://duckdb.org/2024/08/19/duckdb-tricks-part-1.html#shuffling-data
        for more information.

        See [Ibis's documentation on .sample()](https://ibis-project.org/reference/expression-tables.html#ibis.expr.types.relations.Table.sample)
        for more details.
    seed
        The random seed to use for sampling. If None, use a random seed.
    """
    if method is None:
        method = "row" if n_approx <= 2048 * 8 else "block"
    if method == "hash":
        return table.order_by(row_hash(table, seed=seed)).limit(n_approx)
    n_available = table.count().execute()
    fraction = n_approx / n_available
    return table.sample(fraction, method=method, seed=seed)


def row_hash(t: ir.Table, *, seed: int | None = None) -> ir.IntegerColumn:
    """Get a pseudorandom hash of each row in the table.

    This is useful for creating a unique identifier for each row
    that is stable across different runs of the same code.

    Based on the method described at
    https://duckdb.org/2024/08/19/duckdb-tricks-part-1.html#shuffling-data
    """
    # TODO: get this optimization to work
    # if t.get_name() in t._find_backend(use_default=True).tables:
    #     # This is a physical table, we can use the rowid
    #     key = t.rowid()
    #     if seed is not None:
    #         key = key + seed
    #     return key.hash()
    fields = {col: t[col].hash() for col in t.columns}
    if seed is not None:
        fields[unique_name()] = ibis.literal(seed)
    return ibis.struct(fields).hash()


def group_id(
    keys: str | ir.Column | Iterable[str | ir.Column], *, dtype="!uint64"
) -> ir.IntegerColumn:
    """Number each group from 0 to "number of groups - 1".

    This is equivalent to pandas.DataFrame.groupby(keys).ngroup().
    The returned values is of type `dtype`.
    NULLs in the input are labeled just like any other value.
    """
    return ibis.dense_rank().over(ibis.window(order_by=keys)).cast(dtype)


def unique_name(prefix: str | None = None) -> str:
    """Find a universally unique name"""
    if prefix is None:
        prefix = "__temp__"

    uid = base64.b32encode(uuid.uuid4().bytes).decode().rstrip("=").lower()

    return f"{prefix}_{uid}"


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

    int_col_name = unique_name("int_col")
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


def check_schemas_equal(a: ibis.Schema | ibis.Table, b: ibis.Schema | ibis.Table):
    if isinstance(a, ibis.Table):
        a = a.schema()
    if isinstance(b, ibis.Table):
        b = b.schema()

    errs = ["Schemas are not equal"]
    if missing_a := set(b) - set(a):
        errs.append(f"Missing columns from left: {missing_a}")
    if missing_b := set(a) - set(b):
        errs.append(f"Missing columns from right: {missing_b}")
    if conflicting := [c for c in set(b) & set(a) if a[c] != b[c]]:
        errs.extend(
            f"Conflicting dtype for column {c}: {a[c]} != {b[c]}" for c in conflicting
        )

    if len(errs) != 1:
        raise ValueError("\n".join(errs))
