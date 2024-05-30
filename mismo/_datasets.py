from __future__ import annotations

from typing import Callable, Iterable, Mapping, Sequence

import ibis
from ibis.expr import types as ir


class Datasets:
    """An ordered, dict-like collection of tables of records.

    All tables must have a column named 'record_id' that is globally unique.
    The dtype of the 'record_id' column must be the same in all tables.
    Besides that, the schema of the tables can be different.

    This is a nice abstraction over the fact that some record linkage problems
    are deduplication, and thus involve only one table, while others are
    linkage, and involve two tables.
    """

    def __init__(
        self, tables: ir.Table | Iterable[ir.Table] | Mapping[str, ir.Table]
    ) -> None:
        """Create a new Datasets.

        If `tables` is Mapping, then it is used as-is.
        If `tables` is a single table, it is named "dataset_0".
        If `tables` is an iterable of tables, then we try to find their names by:
        - calling `get_name()` on each table. If that fails, then fall back to...
        - using "left" and "right" for two tables
        - using "dataset_i" otherwise
        """
        if isinstance(tables, ir.Table):
            tables = {"dataset_0": tables}
        tables = _get_names(tables)

        self._tables = {}
        for name, t in tables.items():
            if "record_id" not in t.columns:
                raise ValueError("records must have a record_id column")
            # should we .cache() here to guarantee consistency?
            self._tables[name] = t
        if "record_id" not in self.shared_schema:
            raise ValueError("The record_id column must be the same type in all tables")

    def __len__(self) -> int:
        """The number of tables."""
        return len(self._tables)

    def __getitem__(self, key: str | int) -> ir.Table:
        """Get a table by name or index."""
        if isinstance(key, int):
            return tuple(self)[key]
        return self._tables[key]

    def __iter__(self) -> Iterable[ir.Table]:
        """Iterate over the tables in the order they were added."""
        return iter(self._tables.values())

    def __contains__(self, key: str | ir.Table) -> bool:
        """Check if a table is in the collection by name or value."""
        if isinstance(key, str):
            return key in self._tables
        return key in self._tables.values()

    def __repr__(self) -> str:
        return repr({name: table.head(5) for name, table in self.items()})

    def cache(self) -> Datasets:
        """Return a new Datasets with all tables cached."""
        return self.__class__({name: t.cache() for name, t in self.items()})

    def map(self, f: ibis.Deferred | Callable[[str, ir.Table], ir.Table]) -> ir.Table:
        """Return a new Datasets with all tables transformed by `f`."""
        if isinstance(f, ibis.Deferred):
            return self.__class__({name: f.bind(t) for name, t in self.items()})
        else:
            return self.__class__({name: f(name, t) for name, t in self.items()})

    def filter(
        self, f: ibis.Deferred | Callable[[str, ir.Table], ir.Table]
    ) -> ir.Table:
        """Return a new Datasets with all tables filtered by `f`."""
        if isinstance(f, ibis.Deferred):
            return self.__class__({name: t.filter(f) for name, t in self.items()})
        else:
            return self.__class__(
                {name: t.filter(f(name, t)) for name, t in self.items()}
            )

    @property
    def names(self) -> tuple[str, ...]:
        """The names of the underlying tables."""
        return tuple(self._tables.keys())

    @property
    def tables(self) -> tuple[ir.Table, ...]:
        """The underlying tables."""
        return tuple(self)

    def keys(self) -> Iterable[str]:
        """The names of the underlying tables."""
        return self._tables.keys()

    def values(self) -> Iterable[ir.Table]:
        """The underlying tables."""
        return self._tables.values()

    def items(self) -> Iterable[tuple[str, ir.Table]]:
        """The names and tables of the underlying tables."""
        return self._tables.items()

    @property
    def shared_schema(self) -> ibis.Schema:
        """The schema that all tables have in common.

        Columns with conflicting types are omitted.

        This is useful for operations that require the same schema in all tables,
        for example getting all the record_ids.
        """
        dtypes_per_col = {}
        for t in self:
            for col, dtype in t.schema().items():
                if col not in dtypes_per_col:
                    dtypes_per_col[col] = [dtype]
                else:
                    dtypes_per_col[col].append(dtype)
        good = {
            col: dtypes[0]
            for col, dtypes in dtypes_per_col.items()
            if len(dtypes) == len(self) and len(set(dtypes)) == 1
        }
        return ibis.Schema(good)

    def unioned(self) -> ir.Table:
        """Select the `self.shared_schema` columns from all tables and union them."""
        return ibis.union(
            *(
                t.select(*self.shared_schema).mutate(dataset=ibis.literal(name))
                for name, t in self.items()
            )
        )

    def all_record_ids(self) -> ir.Column:
        """Return all unique record_ids from all tables."""
        return self.unioned().select("record_id").distinct().record_id


def _get_names(
    tables: Sequence[ir.Table] | Mapping[str, ir.Table],
) -> dict[str, ir.Table]:
    def _get_name(i: int, t: ir.Table) -> str:
        try:
            ds = t.get_name()
        except AttributeError:
            pass
        else:
            if not ds.startswith("ibis_cache"):
                return ds

        if len(tables) == 2:
            return "left" if i == 0 else "right"
        else:
            return f"dataset_{i}"

    try:
        return dict(tables.items())
    except AttributeError:
        return {_get_name(i, t): t for i, t in enumerate(tables)}
