from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Literal

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _util, joins
from mismo.types._table_wrapper import TableWrapper

if TYPE_CHECKING:
    from collections.abc import Mapping


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

            If None, and the schemas of the before and after tables are dufferent,
            then we return True (ie, keep all rows), because by definition
            every row is different, since the schemas are different.

            If None, and the schemas of the before and after tables are the same,
            then we consider all columns in both before and after tables.

            If you joined on an eg id column, you almost definitely want to exclude it here.

        Examples
        --------
        >>> u = Updates.from_tables(before, after, join_on="id")  # doctest: +SKIP
        >>> u.filter(u.filters.all_different(["name", "age"]))  # doctest: +SKIP
        >>> u.filter(
        ...     u.filters.all_different([c for c in u.columns if c != "my_id"])
        ... )  # doctest: +SKIP
        """  # noqa: E501

        def filter_func(table: ir.Table):
            nonlocal subset
            if subset is None:
                u = Updates(table, check_schemas="lax")
                if u.before().schema() != u.after().schema():
                    return True
                subset = table.columns
            return ibis.and_(*(is_changed(table[col]) for col in subset))

        return filter_func

    @staticmethod
    def any_different(subset: Iterable[str] | None = None):
        """Make a Updates filter function that gives rows where any column is different.

        Parameters
        ----------
        subset : Iterable[str] | None
            The columns to consider.

            If None, and the schemas of the before and after tables are dufferent,
            then we return True (ie, keep all rows), because by definition
            every row is different, since the schemas are different.

            If None, and the schemas of the before and after tables are the same,
            then we consider all columns in both before and after tables.

        Examples
        --------
        >>> u = Updates.from_tables(before, after, join_on="id")  # doctest: +SKIP
        >>> u.filter(u.filters.any_different(["name", "age"]))  # doctest: +SKIP
        """

        def filter_func(table: ir.Table):
            nonlocal subset
            if subset is None:
                u = Updates(table, check_schemas="lax")
                if u.before().schema() != u.after().schema():
                    return True
                subset = table.columns
            return ibis.or_(*(is_changed(table[col]) for col in subset))

        return filter_func


class Updates(TableWrapper):
    """A Table representing how individual rows were updated.

    This only represents differences in rows that exist both before and after the changes.
    To represent a general difference between two tables, eg with insertions and deletions,
    use [Diff](mismo.Diff).

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
        /,
        *,
        check_schemas: Literal["exactly", "names", "lax"] = "exactly",
    ) -> None:
        """Create an Updates object from a table of differences.

        Parameters
        ----------
        diff_table : ibis.Table
            A table with columns that are structs with at least 'before' and 'after' fields.
        check_schemas : Literal["exactly", "names", "lax"]
            How to check the schemas of the before and after values.
            - "exactly": both before and after must have the same columns and types.
            - "names": both before and after must have the same columns, but types can differ.
            - "lax": no schema checking, just that there is at least one of 'before' or 'after' in each column.
        """  # noqa: E501

        def _check_col(name: str):
            col_type: dt.DataType = diff_table[name].type()
            if not isinstance(col_type, dt.Struct):
                return f"Column {name} is not a struct"
            fields = set(col_type.names)
            if "before" not in fields and "after" not in fields:
                return f"Column {name} needs to have at least one of 'before' and 'after' field"  # noqa: E501
            if check_schemas == "exactly":
                if "before" not in fields or "after" not in fields:
                    return (
                        f"Column {name} needs to have both 'before' and 'after' fields"
                    )
                b_type = col_type.fields["before"]
                a_type = col_type.fields["after"]
                if b_type != a_type:
                    return f"Column {name} needs to have the same type for 'before' ({b_type}) and 'after' ({a_type})"  # noqa: E501
            elif check_schemas == "names":
                if "before" not in fields or "after" not in fields:
                    return (
                        f"Column {name} needs to have both 'before' and 'after' fields"
                    )
            elif check_schemas == "lax":
                pass
            else:
                raise ValueError(f"Unknown schema {check_schemas}")

        errors = [_check_col(col_name) for col_name in diff_table.columns]
        errors = [e for e in errors if e is not None]
        if errors:
            raise ValueError("\n".join(errors))

        object.__setattr__(self, "_check_schemas", check_schemas)
        super().__init__(diff_table)

    @property
    def check_schemas(self) -> Literal["exactly", "names", "lax"]:
        """The schema checking mode used for this Updates object."""
        return self._check_schemas

    @classmethod
    def from_tables(
        cls,
        before: ibis.Table,
        after: ibis.Table,
        *,
        join_on: str | Literal[False],
        check_schemas: Literal["exactly", "names", "lax"] = "exactly",
    ) -> Updates:
        """Create from two different tables by joining them on a key.

        Note that this results in only the rows that are present in both tables,
        due to the inner join on the key. Insertions and deletions should be
        handled separately.
        """
        # Prefer a column order of
        # 1. all the columns in after
        # 2. any extra columns in before are tacked on the end
        if after is before:
            after = after.view()
        all_columns = (dict(before.schema()) | dict(after.schema())).keys()
        joined = joins.join(
            before, after, join_on, lname="{name}_l", rname="{name}_r", rename_all=True
        )

        def make_diff_col(col: str) -> ir.StructColumn:
            d = {}
            col_l = col + "_l"
            col_r = col + "_r"
            if col_l in joined.columns:
                # need to do this cast because nonnull dtypes
                # become nullable after the join.
                d["before"] = joined[col_l].cast(before.schema()[col])
            if col_r in joined.columns:
                d["after"] = joined[col_r].cast(after.schema()[col])
            assert d, f"Column {col} not found in either before or after"
            return ibis.struct(d).name(col)

        diff_table = joined.select(*[make_diff_col(c) for c in all_columns])
        return cls(diff_table, check_schemas=check_schemas)

    @classmethod
    def from_before_after(
        cls,
        before: Mapping[str, ibis.Value] | ir.StructValue,
        after: Mapping[str, ibis.Value] | ir.StructValue,
        *,
        check_schemas: Literal["exactly", "names", "lax"] = "exactly",
    ) -> Updates:
        """Create an Updates object from before and after values.

        Parameters
        ----------
        before : Mapping[str, ibis.Value] | ir.StructValue
            The values before the changes.
        after : Mapping[str, ibis.Value] | ir.StructValue
            The values after the changes.
        check_schemas : Literal["exactly", "names", "lax"]
            How to check the schemas of the before and after values.
            - "exactly": both before and after must have the same columns and types.
            - "names": both before and after must have the same columns, but types can differ.
            - "lax": no schema checking, just that there is at least one of 'before' or 'after' in each column.

        Returns
        -------
        Updates
            An Updates object representing the changes.
        """  # noqa: E501
        if isinstance(before, ir.StructValue):
            before = {col: before[col] for col in before.type().names}
        if isinstance(after, ir.StructValue):
            after = {col: after[col] for col in after.type().names}

        all_columns = set(before.keys()) | set(after.keys())

        def make_diff_col(col: str) -> ir.StructColumn:
            d = {}
            if col in before:
                d["before"] = before[col]
            if col in after:
                d["after"] = after[col]
            return ibis.struct(d).name(col)

        diff_table = _util.select(*[make_diff_col(c) for c in all_columns])
        return cls(diff_table, check_schemas=check_schemas)

    def before_values(self) -> dict[str, ir.Column]:
        """The values before the changes."""
        return {
            col: self[col].before
            for col in self.columns
            if "before" in self[col].type().names
        }

    def after_values(self) -> dict[str, ir.Column]:
        """The values after the changes."""
        return {
            col: self[col].after
            for col in self.columns
            if "after" in self[col].type().names
        }

    def before(self) -> ibis.Table:
        """The table before the changes."""
        return self.select(**self.before_values())

    def after(self) -> ibis.Table:
        """The table after the changes."""
        return self.select(**self.after_values())

    def is_changed(self, column: str, /) -> ibis.ir.BooleanColumn:
        """Is column.before different from column.after? Never returns NULL."""
        (val,) = self.bind(column)
        return is_changed(val)

    def filter(self, *args, **kwargs):
        return self.__class__(
            self._t.filter(*args, **kwargs), check_schemas=self.check_schemas
        )

    def cache(self):
        return self.__class__(self._t.cache(), check_schemas=self.check_schemas)

    def apply_to(
        self,
        t: ibis.Table,
        /,
        *,
        defaults: None | Any = _util.NOT_SET,
    ) -> ibis.Table:
        """Return the input table with these updates applied to it.

        Parameters
        ----------
        t
            The table to apply the updates to.
        defaults
            If the after table has more columns than the before table, you must provide defaults.
            This is because there might be some rows in `t` that are not touched by this
            Updates. We need to know what to put in those columns for the untouched rows.
            This accepts anything that `ibis.Table.mutate()` accepts.

            If None, as convenience, we will use `null` as the default for all new columns.
            If _util.NOT_SET, an error is raised if the after table has more columns than the before table.
        """  # noqa: E501
        _util.check_schemas_equal(t, self.before())

        t = t.difference(self.before(), distinct=False)

        if self.before().schema() == self.after().schema():
            # easy, we don't have to worry about adding defaults
            return t.union(self.after(), distinct=False)

        missing_cols = [
            c for c in self.after().schema() if c not in self.before().schema()
        ]
        if missing_cols:
            if defaults is _util.NOT_SET:
                raise ValueError(
                    "If the after table has more columns than the before table, you must provide defaults"  # noqa: E501
                )
            if defaults is None:
                defaults = {c: None for c in missing_cols}

            defaults_cols = t.select(defaults).columns
            already_there = [c for c in defaults_cols if c in t.columns]
            if already_there:
                raise ValueError(
                    f"default value {already_there} already exist in the input table"
                )

            t = t.mutate(defaults)

        t = t.select(self.after().columns)
        t = t.union(self.after(), distinct=False)
        return t


def is_changed(val: ibis.Value, /) -> ibis.ir.BooleanColumn:
    """Is val.before different from val.after? Never returns NULL."""
    return ibis.or_(
        (val.before != val.after).fill_null(False),
        val.before.isnull() != val.after.isnull(),
    )
