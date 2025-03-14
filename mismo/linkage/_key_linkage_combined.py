from __future__ import annotations

from typing import Iterable

import ibis

from mismo import _typing
from mismo.linkage import union
from mismo.linkage._key_linker import KeyLinkage
from mismo.linkage._linkage import Linkage


class UnionKeyLinkage(Linkage):
    """A Linkage based on the union of KeyLinkages."""

    def __init__(self, key_linkages: Iterable[KeyLinkage], labels: bool = False):
        """Create a UnionBlocker from the given Blockers.

        Parameters
        ----------
        blockers
            The Blockers to use.
        labels
            If True, a column of type `array<string>` will be added to the
            resulting table indicating which
            rules caused each record pair to be blocked.
            If False, the resulting table will only contain the columns of left and
            right.
        """
        self.key_linkages = tuple(key_linkages)
        self.labels = labels

    @property
    def left(self) -> ibis.Table:
        return self.key_linkages[0].left

    @property
    def right(self) -> ibis.Table:
        return self.key_linkages[0].right

    @property
    def links(self) -> ibis.Table:
        # TODO: I think we should be efficient here and do something like
        # union(
        #     linkages[0].links,
        #     <all the links in linkages[1] except add an additional join condition to
        #     not include links that are already in the first linkage>
        #     <all the links in linkages[2] except add an additional join condition to
        #     not include links that are already in the first or second linkage>
        #     <etc...>
        # )
        return ibis.union(
            *[linkage.links for linkage in self.key_linkages],
            distinct=True,
        )

    def cache(self) -> _typing.Self:
        """
        No-op to cache this, since the condition is an abstract condition, and
        left and right are already cached.
        """
        # I think this API is fine to just return self instead of a new instance?
        return self


def _all_are_key_linkages(*linkages: Linkage) -> bool:
    return all(isinstance(linkage, KeyLinkage) for linkage in linkages)


def register_combiners():
    union.register(_all_are_key_linkages, lambda *linkages: UnionKeyLinkage(linkages))
    # TODO: add the Difference and Intersection combinators
