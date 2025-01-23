from __future__ import annotations

from typing import Any, Iterable, Literal, TypedDict

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo.types._table_wrapper import TableWrapper


class FieldUpdateDict(TypedDict):
    """A dict representing how a field in a row changed"""

    before: Any
    after: Any


class Filters:
    """A simple namespace for filter functions."""

    @staticmethod
    def all_different(subset: Iterable[str] | None = None):
        """
        Make a Updates filter function that gives rows where all columns are different.

        Parameters
        ----------
        subset : Iterable[str] | None
            The columns to consider.
            If None, we consider all columns in both before and after tables.

        Examples
        --------
        >>> u = Updates.from_tables(before, after, join_on="id")  # doctest: +SKIP
        >>> u.filter(u.filters.all_different(["name", "age"]))  # doctest: +SKIP
        """

        def filter_func(table: ir.Table):
            nonlocal subset
            if subset is None:
                subset = _columns_in_both(table)
            preds = [table[col].before != table[col].after for col in subset]
            return ibis.and_(*preds)

        return filter_func

    @staticmethod
    def any_different(subset: Iterable[str] | None = None):
        """Make a Updates filter function that gives rows where any column is different.

        Parameters
        ----------
        subset : Iterable[str] | None
            The columns to consider.
            If None, we consider all columns in both before and after tables.

        Examples
        --------
        >>> u = Updates.from_tables(before, after, join_on="id")  # doctest: +SKIP
        >>> u.filter(u.filters.any_different(["name", "age"]))  # doctest: +SKIP
        """

        def filter_func(table: ir.Table):
            nonlocal subset
            if subset is None:
                subset = _columns_in_both(table)
            preds = [table[col].before != table[col].after for col in subset]
            return ibis.or_(*preds)

        return filter_func


class Updates(TableWrapper):
    """A Table representing how individual rows were updated.

    This only represents differences in rows that exist both before and after the changes.
    To represent a general difference between two tables, eg with insertions and deletions,
    use [Diff](mismo.types.Diff).

    This represents how each column has changed between two tables.
    If a column only has a 'before' field, it means this column was removed.
    If a column only has an 'after' field, it means this column was added.
    If a column has both 'before' and 'after' fields, it means this column was present in both tables.
    """  # noqa: E501

    filters = Filters
    """A set of filters for convenience.
    
    Examples
    --------
    >>> u = Updates.from_tables(before, after, join_on="id")  # doctest: +SKIP
    >>> u.filter(u.filters.all_different(["name", "age"]))  # doctest: +SKIP
    """

    def __init__(
        self,
        diff_table: ibis.Table,
        *,
        schema: Literal["exactly", "names", "lax"] = "exactly",
    ) -> None:
        def _check_col(name: str):
            col_type: dt.DataType = diff_table[name].type()
            if not isinstance(col_type, dt.Struct):
                return f"Column {name} is not a struct"
            fields = set(col_type.names)
            if "before" not in fields and "after" not in fields:
                return f"Column {name} needs to have at least one of 'before' and 'after' field"  # noqa: E501
            if schema == "exactly":
                if "before" not in fields or "after" not in fields:
                    return (
                        f"Column {name} needs to have both 'before' and 'after' fields"
                    )
                b_type = col_type.fields["before"]
                a_type = col_type.fields["after"]
                if b_type != a_type:
                    return f"Column {name} needs to have the same type for 'before' ({b_type}) and 'after' ({a_type})"  # noqa: E501
            elif schema == "names":
                if "before" not in fields or "after" not in fields:
                    return (
                        f"Column {name} needs to have both 'before' and 'after' fields"
                    )
            elif schema == "lax":
                pass
            else:
                raise ValueError(f"Unknown schema {schema}")

        errors = [_check_col(col_name) for col_name in diff_table.columns]
        errors = [e for e in errors if e is not None]
        if errors:
            raise ValueError("\n".join(errors))

        super().__init__(diff_table)

    @classmethod
    def from_tables(
        cls,
        before: ibis.Table,
        after: ibis.Table,
        *,
        join_on: str,
        schema: Literal["exactly", "names", "lax"] = "exactly",
    ) -> Updates:
        """Create from two different tables by joining them on a key.

        Note that this results in only the rows that are present in both tables,
        due to the inner join on the key. Insertions and deletions should be
        handled separately.
        """
        # Prefer a column order of
        # 1. all the columns in after
        # 2. any extra columns in before are tacked on the end
        all_columns = (dict(after.schema()) | dict(after.schema())).keys()
        joined = ibis.join(
            before,
            after,
            how="inner",
            lname="{name}_l",
            rname="{name}_r",
            predicates=join_on,
        )
        joined = _ensure_suffixed(before.columns, after.columns, joined)

        def make_diff_col(col: str) -> ir.StructColumn:
            d = {}
            col_l = col + "_l"
            col_r = col + "_r"
            if col_l in joined.columns:
                d["before"] = joined[col_l]
            if col_r in joined.columns:
                d["after"] = joined[col_r]
            assert d, f"Column {col} not found in either before or after"
            return ibis.struct(d).name(col)

        diff_table = joined.select(*[make_diff_col(c) for c in all_columns])
        return cls(diff_table, schema=schema)

    def before(self) -> ibis.Table:
        """The table before the changes."""
        return self.select(
            [
                self[col].before.name(col)
                for col in self.columns
                if "before" in self[col].type().names
            ]
        )

    def after(self) -> ibis.Table:
        """The table after the changes."""
        return self.select(
            [
                self[col].after.name(col)
                for col in self.columns
                if "after" in self[col].type().names
            ]
        )

    def filter(self, *args, **kwargs):
        return self.__class__(self._t.filter(*args, **kwargs), schema="lax")

    def cache(self):
        return self.__class__(self._t.cache(), schema="lax")

    def as_row_update_dicts(
        self, chunk_size: int = 1000000
    ) -> Iterable[dict[str, FieldUpdateDict]]:
        """Iterate through how every row changed."""
        for batch in self.to_pyarrow_batches(chunk_size=chunk_size):
            yield from batch.to_pylist()


def _ensure_suffixed(
    original_left_cols: Iterable[str], original_right_cols: Iterable[str], t: ir.Table
) -> ir.Table:
    """Ensure that all columns in `t` have a "_l" or "_r" suffix."""
    lc = set(original_left_cols)
    rc = set(original_right_cols)
    just_left = lc - rc
    just_right = rc - lc
    m = {c + "_l": c for c in just_left} | {c + "_r": c for c in just_right}
    t = t.rename(m)

    # If the condition is an equality condition, like `left.name == right.name`,
    # then since we are doing an inner join ibis doesn't add suffixes to these
    # columns. So we need duplicate these columns and add suffixes.
    un_suffixed = [
        c for c in t.columns if not c.endswith("_l") and not c.endswith("_r")
    ]
    m = {c + "_l": ibis._[c] for c in un_suffixed} | {
        c + "_r": ibis._[c] for c in un_suffixed
    }
    t = t.mutate(**m).drop(*un_suffixed)
    return t


def _columns_in_both(t: ibis.Table) -> tuple[str]:
    """The columns that were in both the original and new table."""
    return tuple(
        name
        for name, typ in t.schema().items()
        if "before" in typ.names and "after" in typ.names
    )
