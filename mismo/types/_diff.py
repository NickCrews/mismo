from __future__ import annotations

import ibis

from mismo.types import Updates


class Diff:
    """A set of insertions, updates, and deletions between two tables."""

    def __init__(
        self, before: ibis.Table, after: ibis.Table, *, join_on: str | None = None
    ):
        """Create a set of changes between two tables."""
        self._before = before
        self._after = after
        self._join_on = join_on

    @classmethod
    def from_deltas(
        cls,
        *,
        before: ibis.Table,
        insertions: ibis.Table | None = None,
        updates: ibis.Table | None = None,
        deletions: ibis.Table | None = None,
        join_on: str | None = None,
    ):
        """Create from a starting point and a set of transformations."""
        after = before
        if deletions is not None:
            after = before.anti_join(deletions, join_on)
        if updates is not None:
            after = ibis.union(after.anti_join(updates, join_on), updates)
        if insertions is not None:
            after = ibis.union(after, insertions)
        return cls(before, after=after, join_on=join_on)

    def cache(self) -> Diff:
        """Cache the tables in the changes."""
        return Diff(
            before=self._before.cache(),
            after=self._after.cache(),
            join_on=self.join_on(),
        )

    def before(self) -> ibis.Table:
        """The table before the changes."""
        return self._before

    def after(self) -> ibis.Table:
        """The table after the changes."""
        return self._after

    def join_on(self) -> str:
        """The key to join the before and after tables."""
        return self._join_on

    def updates(self) -> Updates:
        """Rows that were updated between `before` and `after`."""
        return Updates.from_tables(self._before, self._after, join_on=self.join_on())

    def insertions(self) -> ibis.Table:
        """Rows that were in `after` but not in `before`."""
        return self._after.anti_join(self._before, self.join_on())

    def deletions(self) -> ibis.Table:
        """Rows that were in `before` but not in `after`."""
        return self._before.anti_join(self._after, self.join_on())
