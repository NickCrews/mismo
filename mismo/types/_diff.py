from __future__ import annotations

import functools
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Literal

import ibis

from mismo import _util
from mismo.types._updates import Updates

if TYPE_CHECKING:
    import altair as alt

    from mismo import _typing


class Diff:
    """A set of insertions, updates, and deletions between two tables.

    This can only semantically represent 1-1 relationships,
    eg a row in the `before` table corresponds to only 0 or 1 row in the `after` table,
    and vice versa. eg "this row changed in these ways between these two tables".

    To represent more general 0-N relationships, use a [Linkage](mismo.Linkage).
    eg many rows in a "dirty" dataset are linked to a single row in a "clean" dataset.
    Say you have a clean database of records.
    You just got a new batch of dirty data that might contain duplicates.
    Each record in the clean database might match multiple records in the dirty data.
    This makes it difficult to use a Diff object, because each clean record
    can't be paired up nicely with a single dirty record.
    A Linkage object is more appropriate in this case.

    This is able to represent a difference between two tables with different schemas,
    eg if a column is added or removed.
    """

    def __new__(*args, **kwargs):
        raise NotImplementedError(
            "Use one of the Diff.from_*() factory methods to create a Diff object."
        )

    @classmethod
    def _new(
        cls,
        before: ibis.Table,
        after: ibis.Table,
        insertions: ibis.Table,
        updates: Updates,
        deletions: ibis.Table,
    ):
        assert isinstance(before, ibis.Table)
        assert isinstance(after, ibis.Table)
        assert isinstance(insertions, ibis.Table)
        assert updates.__class__.__name__ == "Updates"  # isinstance doesn't work??
        assert isinstance(deletions, ibis.Table)

        _check_schemas_equal(before, deletions)
        _check_schemas_equal(before, updates.before())
        _check_schemas_equal(after, insertions)
        _check_schemas_equal(after, updates.after())
        # before and after may have different schemas.

        # only keep updates that actually change something
        updates = updates.filter(updates.filters.any_different())

        obj = super().__new__(cls)
        obj.__init__()
        obj._before = before
        obj._after = after
        obj._insertions = insertions
        obj._updates = updates
        obj._deletions = deletions
        return obj

    @classmethod
    def from_deltas(
        cls,
        *,
        before: ibis.Table,
        insertions: ibis.Table | None = None,
        updates: Updates | None = None,
        deletions: ibis.Table | None = None,
        updates_defaults: None | Any = _util.NOT_SET,
    ) -> _typing.Self:
        """Create from a starting point and a set of transformations.

        Parameters
        ----------
        before
            The table before the changes.
        insertions
            Rows that were in `after` but not in `before`.
            If None, we assume there are no insertions.
        updates
            Rows that were changed between `before` and `after`.
            If None, we assume there are no updates.
        deletions
            Rows that were in `before` but not in `after`.
            If None, we assume there are no deletions.

        Returns
        -------
        Diff
        """
        after = before
        if deletions is not None:
            after = before.difference(deletions, distinct=False)
        else:
            deletions = before.limit(0)

        if updates is not None:
            after = updates.apply_to(after, defaults=updates_defaults)
        else:
            updates = Updates.from_tables(before, after, join_on=False)

        if insertions is not None:
            after = after.union(insertions, distinct=False)
        else:
            insertions = after.limit(0)

        return cls._new(
            before=before,
            after=after,
            insertions=insertions,
            updates=updates,
            deletions=deletions,
        )

    @classmethod
    def from_before_after(
        cls,
        before: ibis.Table,
        after: ibis.Table,
        *,
        join_on: str | Literal[False],
    ) -> _typing.Self:
        """Create from a before and after table."""
        # We have to be careful here.
        # If a row (Bob, 123, 456) is updated to (Bob, 123, 789),
        # Then is this a deletion and insertion, or an update?
        # We have to rely on the join key to determine this,
        # using set-based difference(), intersection(), etc would not work.
        if join_on is False:
            insertions = after
            deletions = before
        else:
            insertions = after.anti_join(before, join_on)
            deletions = before.anti_join(after, join_on)
        updates = Updates.from_tables(before, after, join_on=join_on)
        return cls._new(
            before=before,
            after=after,
            insertions=insertions,
            updates=updates,
            deletions=deletions,
        )

    def cache(self) -> Diff:
        """Return a new Diff with all tables cached."""
        return Diff._new(
            before=self.before().cache(),
            after=self.after().cache(),
            insertions=self.insertions().cache(),
            updates=Updates(self.updates().cache(), check_schemas="lax"),
            deletions=self.deletions().cache(),
        )

    def to_parquets(self, directory: str | Path, /, *, overwrite: bool = False) -> None:
        """Write the tables in the changes to parquet files."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)

        def write(table, name):
            p = directory / f"{name}.parquet"
            if not overwrite and p.exists():
                raise FileExistsError(f"{p} already exists")
            table.to_parquet(p)

        # TODO: this isn't atomic, if one fails, the others will still be written.
        write(self.before(), "before")
        write(self.insertions(), "insertions")
        write(self.deletions(), "deletions")
        write(self.updates(), "updates")

    @classmethod
    def from_parquets(
        cls, directory: str | Path, /, *, backend: ibis.BaseBackend | None = None
    ) -> _typing.Self:
        """Create a Diff by reading parquets from the given directory."""
        if backend is None:
            backend = ibis
        d = Path(directory)
        return cls.from_deltas(
            before=backend.read_parquet(d / "before.parquet"),
            insertions=backend.read_parquet(d / "insertions.parquet"),
            deletions=backend.read_parquet(d / "deletions.parquet"),
            updates=Updates(
                backend.read_parquet(d / "updates.parquet"), check_schemas="lax"
            ),
        )

    def before(self) -> ibis.Table:
        """The table before the changes."""
        return self._before

    def after(self) -> ibis.Table:
        """The table after the changes."""
        return self._after

    def unchanged(self) -> ibis.Table:
        """Rows that were unchanged between `before` and `after`."""
        return self.before().difference(
            self.updates().before(),
            self.deletions(),
        )

    def insertions(self) -> ibis.Table:
        """Rows that were in `after` but not in `before`.

        This has the same schema as `after`."""
        return self._insertions

    def deletions(self) -> ibis.Table:
        """Rows that were in `before` but not in `after`.

        This has the same schema as `before`.
        """
        return self._deletions

    def updates(self) -> Updates:
        """Rows that were changed between `before` and `after`.

        `self.updates().before()` and `self.updates().after()` have the same schemas
        as `before` and `after`, respectively."""
        return self._updates

    def chart(self) -> alt.Chart:
        """Create a chart that shows the flow of rows through the diff.

        This is just a convenience method that calls `self.stats.chart()`.

        ```plaintext
        Rows
        800,000 |                                 ▓▓  Inserted (50,000)
                |                                 ▒▒  Deleted (100,000)
        700,000 |                                 ░░  Updated (200,000)
                |                                 ██  Unchanged (300,000)
        600,000 |      ▒▒▒▒
                |      ▒▒▒▒             ▓▓▓▓
        500,000 |      ░░░░             ░░░░
                |      ░░░░             ░░░░
        400,000 |      ░░░░             ░░░░
                |      ░░░░             ░░░░
        300,000 |      ████             ████
                |      ████             ████
        200,000 |      ████             ████
                |      ████             ████
        100,000 |      ████             ████
                |      ████             ████
              0 | Before (600,000)  After (550,000)
        ```
        """
        return self.stats.chart()

    @property
    def stats(self) -> DiffStats:
        """Statistics about this Diff."""
        return DiffStats(self)

    def __repr__(self):
        return repr(self.stats).replace("DiffStats", "Diff")


def _check_schemas_equal(table1: ibis.Table, table2: ibis.Table):
    pairs1 = set(dict(table1.schema()).items())
    pairs2 = set(dict(table2.schema()).items())
    if pairs1 == pairs2:
        return
    same = pairs1.intersection(pairs2)
    only1 = pairs1 - pairs2
    only2 = pairs2 - pairs1
    raise ValueError(f"Schemas are not equal: {same=}, {only1=}, {only2=}")


class DiffStats:
    """Summary statistics about a Diff, such as number of insertions, deletions, etc."""

    def __init__(self, diff: Diff) -> None:
        """Create from a Diff."""
        self._diff = diff

    @functools.cache
    def n_before(self) -> int:
        """Number of rows in `before`."""
        return int(self._diff.before().count().execute())

    @functools.cache
    def n_unchanged(self) -> int:
        """Number of rows that were unchanged between `before` and `after`."""
        return int(self._diff.unchanged().count().execute())

    @functools.cache
    def n_insertions(self) -> int:
        """Number of rows that were in `after` but not in `before`."""
        return int(self._diff.insertions().count().execute())

    @functools.cache
    def n_deletions(self) -> int:
        """Number of rows that were in `before` but not in `after`."""
        return int(self._diff.deletions().count().execute())

    @functools.cache
    def n_updates(self) -> int:
        """Number of rows that were changed between `before` and `after`."""
        return int(self._diff.updates().count().execute())

    @functools.cache
    def n_after(self) -> int:
        """Number of rows in `after`."""
        return int(self._diff.after().count().execute())

    def __repr__(self):
        return dedent(f"""
        DiffStats(
            before={self.n_before():_},
            after={self.n_after():_},
            unchanged={self.n_unchanged():_},
            updates={self.n_updates():_},
            insertions={self.n_insertions():_},
            deletions={self.n_deletions():_},
        )""")

    def chart(self) -> alt.Chart:
        """Create a chart that shows the flow of rows through the diff.

        ```plaintext
        Rows
        800,000 |                                 ▓▓  Inserted (50,000)
                |                                 ▒▒  Deleted (100,000)
        700,000 |                                 ░░  Updated (200,000)
                |                                 ██  Unchanged (300,000)
        600,000 |      ▒▒▒▒
                |      ▒▒▒▒             ▓▓▓▓
        500,000 |      ░░░░             ░░░░
                |      ░░░░             ░░░░
        400,000 |      ░░░░             ░░░░
                |      ░░░░             ░░░░
        300,000 |      ████             ████
                |      ████             ████
        200,000 |      ████             ████
                |      ████             ████
        100,000 |      ████             ████
                |      ████             ████
              0 | Before (600,000)  After (550,000)
        ```
        """
        import altair as alt

        unchanged_type = f"Unchanged ({self.n_unchanged():,})"
        inserted_type = f"Inserted ({self.n_insertions():,})"
        deleted_type = f"Deleted ({self.n_deletions():,})"
        updated_type = f"Updated ({self.n_updates():,})"

        before = f"Before ({self.n_before():,})"
        after = f"After ({self.n_after():,})"

        def _percent(a, b) -> float:
            return a / b if b else float("nan")

        data = [
            {
                "table": before,
                "type": "Unchanged",
                "type_order": 1,
                "type_detailed": unchanged_type,
                "count": self.n_unchanged(),
                "fraction": _percent(self.n_unchanged(), self.n_before()),
            },
            {
                "table": before,
                "type": "Deleted",
                "type_order": 3,
                "type_detailed": deleted_type,
                "count": self.n_deletions(),
                "fraction": _percent(self.n_deletions(), self.n_before()),
            },
            {
                "table": before,
                "type": "Updated",
                "type_order": 2,
                "type_detailed": updated_type,
                "count": self.n_updates(),
                "fraction": _percent(self.n_updates(), self.n_before()),
            },
            {
                "table": after,
                "type": "Unchanged",
                "type_order": 1,
                "type_detailed": unchanged_type,
                "count": self.n_unchanged(),
                "fraction": _percent(self.n_unchanged(), self.n_after()),
            },
            {
                "table": after,
                "type": "Inserted",
                "type_order": 3,
                "type_detailed": inserted_type,
                "count": self.n_insertions(),
                "fraction": _percent(self.n_insertions(), self.n_after()),
            },
            {
                "table": after,
                "type": "Updated",
                "type_order": 2,
                "type_detailed": updated_type,
                "count": self.n_updates(),
                "fraction": _percent(self.n_updates(), self.n_after()),
            },
        ]
        data = ibis.memtable(data)
        chart = (
            alt.Chart(data)
            .mark_bar()
            .encode(
                alt.X(
                    "table",
                    title=None,
                    sort=[before, after],
                    # axis=alt.Axis(labelAngle=0),
                ),
                alt.Y("count", title="Rows"),
                # Make the unchanged sit on bottom, then updates, deletions insertions
                alt.Order("type_order"),
                alt.Color(
                    "type_detailed",
                    title=None,
                    sort=[
                        inserted_type,
                        deleted_type,
                        updated_type,
                        unchanged_type,
                    ],
                ),
                tooltip=[
                    "type",
                    alt.Tooltip("count", format=","),
                    alt.Tooltip("fraction", format=".2%"),
                ],
            )
        )
        return chart
