from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Protocol

import ibis
from ibis.expr import datatypes as dt
from ibis.expr import types as ir

from mismo import _typing, _util, joins
from mismo.types._wrapper import StructWrapper, TableWrapper

if TYPE_CHECKING:
    from collections.abc import Mapping

ValueChangeType = Literal[
    "remained_null", "became_null", "became_nonnull", "changed", "unchanged"
]


class UpdatedColumn(StructWrapper):
    """A column representing how a single value was updated.

    This has at least one of the following fields:

    - `before`: The value before the update.
    - `after`: The value after the update.
    """

    before: ir.Value | None
    """The value before the update."""

    after: ir.Value | None
    """The value after the update."""

    def __init__(self, x: ir.StructValue, /) -> None:
        super().__init__(x)
        if "before" not in x.type().names:  # ty:ignore[unresolved-attribute]
            object.__setattr__(self, "before", None)
        if "after" not in x.type().names:  # ty:ignore[unresolved-attribute]
            object.__setattr__(self, "after", None)
        if self.before is None and self.after is None:
            raise ValueError(
                "UpdatedColumn must have at least one of 'before' or 'after'"
            )

    def is_changed(self) -> ir.BooleanValue | Literal[True]:
        """Is `before` different from `after`?

        If this column does not have both before and after fields, returns False,
        because that implies the schema has changed.
        """
        return is_changed(self)

    def schema_change(self) -> Literal["added", "removed", "type_changed", "unchanged"]:
        """Was this column added, removed, have its type changed, or unchanged in the update?"""  # noqa: E501
        if self.before is None and self.after is not None:
            return "added"
        elif self.before is not None and self.after is None:
            return "removed"
        else:
            if self.before.type() != self.after.type():  # ty:ignore[possibly-missing-attribute]
                return "type_changed"
            else:
                return "unchanged"

    def value_change(self) -> ibis.StringValue:
        """How did the value change?

        Returns one of:

        - "remained_null": both before and after are null
        - "became_null": before was not null, after is null
        - "became_nonnull": before was null, after is not null
        - "changed": both before and after are not null, but different
        - "unchanged": both before and after are identical

        Throws
        ------
        ValueError
            If the column was added or removed (ie does not have both before and after fields).
            Check this first with `schema_change()`.
        """  # noqa: E501
        if self.schema_change() in ("added", "removed"):
            raise ValueError(
                "Cannot determine value change for columns that were added or removed"
            )
        before_is_null = self.before.isnull()
        after_is_null = self.after.isnull()
        return (
            ibis.cases(
                (before_is_null & after_is_null, "remained_null"),
                (~before_is_null & after_is_null, "became_null"),
                (before_is_null & ~after_is_null, "became_nonnull"),
                (identical_to(self), "unchanged"),
                else_="changed",
            ),
        )


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

    def __getattr__(self, name: str) -> UpdatedColumn:
        raw = super().__getattr__(name)
        if name in self.__wrapped__.columns:
            return UpdatedColumn(raw)
        return raw

    def __getitem__(self, name: str) -> UpdatedColumn:
        raw = super().__getitem__(name)
        if name in self.__wrapped__.columns:
            return UpdatedColumn(raw)
        return raw

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

    def filter(self, *args, **kwargs) -> _typing.Self:
        return self.__class__(
            self.__wrapped__.filter(*args, **kwargs), check_schemas=self.check_schemas
        )

    def cache(self) -> _typing.Self:
        return self.__class__(
            self.__wrapped__.cache(), check_schemas=self.check_schemas
        )

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

            t = t.mutate(defaults)  # ty:ignore[invalid-argument-type]

        t = t.select(self.after().columns)
        t = t.union(self.after(), distinct=False)
        return t


class HasBeforeAfter(Protocol):
    before: ibis.Value | None
    after: ibis.Value | None


def identical_to(val: HasBeforeAfter, /) -> ibis.ir.BooleanValue | Literal[False]:
    """Literally `val.before.identical_to(val.after)`"""
    if (before := val.before) is None or (after := val.after) is None:
        return False
    return before.identical_to(after)


def is_changed(val: HasBeforeAfter, /) -> ir.BooleanValue | Literal[True]:
    """Is `before` different from `after`?

    If this column does not have both before and after fields, returns False,
    because that implies the schema has changed.
    """
    is_ident = identical_to(val)
    if is_ident is False:
        return True
    return ~is_ident
