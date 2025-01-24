from __future__ import annotations

import ibis

from mismo.types._updates import Updates


class Diff:
    """A set of insertions, updates, and deletions between two tables.

    This can only semantically represent 1-1 relationships,
    eg a row in the `before` table corresponds to only 0 or 1 row in the `after` table,
    and vice versa.

    To represent more general 1-N relationships, use a [Linkage](mismo.types.Linkage).
    eg a row in the `before` table corresponds to 0 or more rows in the `after` table.
    """

    def __new__(*args, **kwargs):
        raise NotImplementedError("Use Diff.from_deltas() to create a Diff object.")

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
    ):
        """Create from a starting point and a set of transformations."""
        after = before
        if deletions is not None:
            after = before.difference(deletions, distinct=False)
        else:
            deletions = before.limit(0)

        if updates is not None:
            after = after.difference(updates.before(), distinct=False)
            after = after.union(updates.after(), distinct=False)
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
        join_on: str,
    ):
        """Create from a before and after table."""
        insertions = after.difference(before, distinct=False)
        deletions = before.difference(after, distinct=False)
        updates = Updates.from_tables(before, after, join_on=join_on)
        return cls._new(
            before=before,
            after=after,
            insertions=insertions,
            updates=updates,
            deletions=deletions,
        )

    def cache(self) -> Diff:
        """Cache the tables in the changes."""
        return Diff._new(
            before=self.before().cache(),
            after=self.after().cache(),
            insertions=self.insertions().cache(),
            updates=Updates(self.updates().cache(), schema="lax"),
            deletions=self.deletions().cache(),
        )

    def before(self) -> ibis.Table:
        """The table before the changes."""
        return self._before

    def after(self) -> ibis.Table:
        """The table after the changes."""
        return self._after

    def unchanged(self) -> Updates:
        """Rows that were unchanged between `before` and `after`."""
        return self.before().intersect(self.after())

    def insertions(self) -> ibis.Table:
        """Rows that were in `after` but not in `before`."""
        return self._insertions

    def deletions(self) -> ibis.Table:
        """Rows that were in `before` but not in `after`."""
        return self._deletions

    def updates(self) -> Updates:
        """Rows that were changed between `before` and `after`."""
        return self._updates


def _check_schemas_equal(table1: ibis.Table, table2: ibis.Table):
    pairs1 = set(dict(table1.schema()).items())
    pairs2 = set(dict(table2.schema()).items())
    if pairs1 == pairs2:
        return
    same = pairs1.intersection(pairs2)
    only1 = pairs1 - pairs2
    only2 = pairs2 - pairs1
    raise ValueError(f"Schemas are not equal: {same=}, {only1=}, {only2=}")
