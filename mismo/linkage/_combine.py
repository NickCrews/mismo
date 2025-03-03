from __future__ import annotations

from typing import Callable, Tuple

import ibis

from mismo.linkage._linkage import BaseLinkage as BaseLinkage
from mismo.linkage._linkage import LinkTableLinkage


class Combiner:
    def __init__(
        self,
        implementations: list[
            Tuple[
                Callable[[tuple[BaseLinkage, ...]], bool],
                Callable[[tuple[BaseLinkage, ...]], BaseLinkage],
            ]
        ] = [],
    ) -> None:
        self._implementations = [i for i in implementations]
        self._implementations.append((lambda *x: True, self.default))

    def register(
        self,
        is_match: Callable[[tuple[BaseLinkage, ...]], bool],
        implementation: Callable[[tuple[BaseLinkage, ...]], BaseLinkage],
    ) -> None:
        """Register a new implementation of the Combiner."""
        self._implementations = [(is_match, implementation), *self._implementations]

    def find(
        self, *linkages: BaseLinkage
    ) -> Callable[[tuple[BaseLinkage, ...]], BaseLinkage]:
        """Find the first implementation that matches the given Linkages."""
        linkages = tuple(linkages)
        check_share_left_and_right(*linkages)
        for is_match, implementation in self._implementations:
            try:
                im = is_match(*linkages)
            except NotImplementedError:
                continue
            if im is NotImplementedError:
                continue
            if im:
                return implementation
        raise NotImplementedError

    def __call__(self, *linkages: BaseLinkage) -> BaseLinkage:
        """Combine two Linkages."""
        return self.find(*linkages)(*linkages)

    def default(self, first: BaseLinkage, *linkages: BaseLinkage) -> BaseLinkage:
        raise NotImplementedError


class Union(Combiner):
    """Return a Linkage that contains links that are in either input Linkages."""

    @staticmethod
    def default(first: BaseLinkage, *linkages: BaseLinkage) -> BaseLinkage:
        links = [first.links, *[x.links for x in linkages]]
        return LinkTableLinkage(
            left=linkages[0].left,
            right=linkages[0].right,
            links=ibis.union(*links),
        )


class Intersect(Combiner):
    """Return a Linkage that contains links that are in both input Linkages."""

    @staticmethod
    def default(first: BaseLinkage, *linkages: BaseLinkage) -> BaseLinkage:
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
    def default(first: BaseLinkage, *linkages: BaseLinkage) -> BaseLinkage:
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


def check_share_left_and_right(*linkages: BaseLinkage) -> None:
    if not linkages:
        raise ValueError("No linkages provided")
    first_left = linkages[0].left
    first_right = linkages[0].right
    for linkage in linkages[1:]:
        if linkage.left is not first_left:
            raise ValueError("left tables do not match")
        if linkage.right is not first_right:
            raise ValueError("right tables do not match")
