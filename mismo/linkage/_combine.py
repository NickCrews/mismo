from __future__ import annotations

from typing import Callable, Protocol, Tuple, runtime_checkable

import ibis

from mismo.linkage._linkage import Linkage, LinkTableLinkage


@runtime_checkable
class PCombiner(Protocol):
    def __call__(self, first: Linkage, /, *linkages: Linkage) -> Linkage: ...


class Combiner:
    def __init__(
        self,
        implementations: list[
            Tuple[
                Callable[[tuple[Linkage, ...]], bool],
                PCombiner,
            ]
        ] = [],
    ) -> None:
        self._implementations = [i for i in implementations]
        self._implementations.append((lambda *x: True, self.default))

    def register(
        self,
        is_match: Callable[[tuple[Linkage, ...]], bool],
        implementation: PCombiner,
    ) -> None:
        """Register a new implementation of the Combiner."""
        self._implementations = [(is_match, implementation), *self._implementations]

    def find(self, first: Linkage, /, *linkages: Linkage) -> PCombiner:
        """Find the first implementation that matches the given Linkages."""
        linkages = tuple(linkages)
        for is_match, implementation in self._implementations:
            try:
                im = is_match(first, *linkages)
            except NotImplementedError:
                continue
            if im is NotImplementedError:
                continue
            if im:
                return implementation
        raise NotImplementedError

    def __call__(self, first: Linkage, /, *linkages: Linkage) -> Linkage:
        """Combine two Linkages."""
        # Should we verify that all the linkages share the same left and right tables?
        return self.find(first, *linkages)(first, *linkages)

    def default(self, first: Linkage, /, *linkages: Linkage) -> Linkage:
        raise NotImplementedError


class Union(Combiner):
    """Return a Linkage that contains links that are in either input Linkages."""

    @staticmethod
    def default(first: Linkage, *linkages: Linkage) -> Linkage:
        links = [first.links, *[x.links for x in linkages]]
        return LinkTableLinkage(
            left=linkages[0].left,
            right=linkages[0].right,
            links=ibis.union(*links),
        )


class Intersect(Combiner):
    """Return a Linkage that contains links that are in both input Linkages."""

    @staticmethod
    def default(first: Linkage, *linkages: Linkage) -> Linkage:
        links = [first.links, *[x.links for x in linkages]]
        return LinkTableLinkage(
            left=linkages[0].left,
            right=linkages[0].right,
            links=ibis.intersect(*links),
        )


class Difference(Combiner):
    """
    Return a Linkage that contains links that are in the left Linkage but not the right.
    """

    @staticmethod
    def default(first: Linkage, *linkages: Linkage) -> Linkage:
        links = [first.links, *[x.links for x in linkages]]
        return LinkTableLinkage(
            left=linkages[0].left,
            right=linkages[0].right,
            links=ibis.difference(*links),
        )


union = Union()
"""Return a Linkage that contains links that are in either input Linkages."""
intersect = Intersect()
"""Return a Linkage that contains links that are in both input Linkages."""
difference = Difference()
"""
Return a Linkage that contains links that are in the left Linkage but not the right.
"""


def unify_links_min_intersection(*linkages: Linkage) -> tuple[Linkage, ...]:
    """
    Given some Linkages, simplify each links to only include the columns that are in all Linkages.

    In order to combine all the `LinksTable`s into a single `LinkTable`,
    we need all their schemas to be the same.
    This is one strategy to do that.

    Another strategy would be to include the columns that are in any of the link tables,
    and fill in the missing columns with nulls.
    """  # noqa: E501
    all_links = [linkage.links for linkage in linkages]
    shared, *rest = [
        set((name, dtype) for name, dtype in link.schema().items())
        for link in all_links
    ]
    for column_set in rest:
        shared &= column_set
    shared_columns = [name for name, _ in shared]
    return tuple(
        linkage.adjust(links=linkage.links.select(*shared_columns))
        for linkage in linkages
    )
