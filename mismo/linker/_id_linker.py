from __future__ import annotations

from typing import Any, Iterable, Literal

import ibis
from ibis.expr import types as ir

from mismo import _resolve
from mismo.linkage._linkage import Linkage, LinksTable
from mismo.linker._key_linker import KeyLinker


class IDLinker:
    """
    Helper for linking unambiguous IDs, such as social security numbers, usernames, etc.

    Where the IDs match, we treat them as a match.

    Where the IDs are uneqal, we can either treat them as a non-match or as a maybe.
    Similar for when one or both of the IDs are null,
    we can either treat them as a non-match or as a maybe.

    For example, say you are linking user accounts.
    If the emails match, then you know they are the same user.
    If they don't match, then they MIGHT be the same user, but you don't know,
    and will have to look at e.g. the IP address, the name on the account, etc.
    So this would be a `when_not_equal="indefinite"` situation.

    On the other hand, say you are linking records of people
    with social security numbers.
    If the SSN match, then you know they are the same person.
    If the SSN don't match, then you know if they are not the same person.
    So this would be a `when_not_equal="nonmatch"` situation.

    Similar for when one or both of the IDs are null.
    Depending on the situation,
    this can either signify that the records are a non-match,
    or that we don't know, and we will leave that decision to other
    parts of the linkage pipeline.

    See https://github.com/NickCrews/mismo/issues/77 for more information.
    """  # noqa: E501

    def __init__(
        self,
        labels: Iterable[str | ibis.Deferred | Any],
        *,
        when_null: Literal["nonmatch", "indefinite"],
        when_not_equal: Literal["nonmatch", "indefinite"],
    ) -> None:
        """Create an IDLinker

        Parameters
        ----------
        labels:
            Something that resolves against the left and right table to the Column
            that holds the labels.
        when_not_equal:
            What to do for record pairs where the labels are not equal.
            If "nonmatch", then we will consider the records to be a non-match.
            If "indefinite", then we don't know if the records are a match or not,
            and will leave that decision to other parts of the pipeline.
        when_null:
            What to do for record pairs where  one or both of them have a null label.
            If "nonmatch", then we will consider the records to be a non-match.
            If "indefinite", then we don't know if the records are a match or not,
            and will leave that decision to other parts of the pipeline.
        """
        if when_null not in ("nonmatch", "indefinite"):
            raise ValueError(
                f"when_null should be either 'nonmatch' or 'indefinite'. Got {when_null}"  # noqa: E501
            )
        if when_not_equal not in ("nonmatch", "indefinite"):
            raise ValueError(
                f"when_not_equal should be either 'nonmatch' or 'indefinite'. Got {when_not_equal}"  # noqa: E501
            )

        self.resolvers = tuple(_resolve.key_pair_resolvers(labels))
        self.when_null = when_null
        self.when_not_equal = when_not_equal

    def match_condition(self, a: ibis.Table, b: ibis.Table) -> ir.BooleanColumn:
        """Select any pairs where we know they are a match (ie the labels are equal)."""
        return KeyLinker(self.resolvers).__join_condition__(a, b)

    def match_linkage(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        if right is left:
            right = right.view()
        links = LinksTable.from_join_condition(left, right, self.match_condition)
        return Linkage(left=left, right=right, links=links)

    # def nonmatch_condition(self, a: ibis.Table, b: ibis.Table) -> ir.BooleanValue:
    #     """Select any pairs where we know they are a non-match.

    #     Includes pairs where either
    #     - one or both of the labels are null (iff `when_null` is "nonmatch")
    #     - the labels are not equal (iff `when_not_equal` is "nonmatch")
    #     """
    #     label_a, label_b = _resolve.resolve_column_pair(self.labels, a, b)
    #     conditions = []
    #     if self.when_not_equal == "nonmatch":
    #         conditions.append(
    #             ibis.and_(
    #                 label_a != label_b,
    #                 label_a.notnull(),
    #                 label_b.notnull(),
    #             )
    #         )
    #     if self.when_null == "nonmatch":
    #         conditions.append(label_a.isnull() | label_b.isnull())
    #     if not conditions:
    #         return ibis.literal(False)
    #     return ibis.or_(*conditions)

    def indefinite_condition(self, a: ibis.Table, b: ibis.Table) -> ir.BooleanValue:
        """Select any pairs where they are not a match and they are not a non-match."""
        raise NotImplementedError()

    def indefinite_linkage(self, left: ibis.Table, right: ibis.Table) -> Linkage:
        if right is left:
            right = right.view()
        links = LinksTable.from_join_condition(left, right, self.indefinite_condition)
        return Linkage(left=left, right=right, links=links)

    def __repr__(self) -> str:
        return f"IDLinker(labels={self.resolvers}, when_null={self.when_null}, when_not_equal={self.when_not_equal})"  # noqa: E501
