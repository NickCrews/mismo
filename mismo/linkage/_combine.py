from __future__ import annotations

import logging
from typing import (
    Callable,
    Generic,
    Iterable,
    Protocol,
    TypeVar,
    runtime_checkable,
)

import ibis

from mismo._registry import Registry
from mismo.joins import HasJoinCondition, join
from mismo.linkage._linkage import Linkage, LinkedTable, LinksTable

logger = logging.getLogger(__name__)

L = TypeVar("L", bound=Linkage)


@runtime_checkable
class HasSublinkages(Protocol, Generic[L]):
    def __sublinkages__(self) -> tuple[L]:
        pass


class CombinedLinkage(Generic[L]):
    def __init__(self, sublinkages: Iterable[L]) -> None:
        self._sublinkages = tuple(sublinkages)
        if not self._sublinkages:
            raise ValueError("Need at least 1 linkage")

    def __sublinkages__(self) -> tuple[L]:
        return self._sublinkages

    @property
    def left(self) -> LinkedTable:
        return self._make_pair[0]

    @property
    def right(self) -> LinkedTable:
        return self._make_pair[1]

    @property
    def links(self) -> LinksTable:
        return LinksTable(self._links_raw, left=self._left_raw, right=self._left_raw)

    @property
    def _make_pair(self):
        return LinkedTable.make_pair(
            left=self._left_raw, right=self._right_raw, links=self._links_raw
        )

    @property
    def _left_raw(self) -> ibis.Table:
        return self._sublinkages[0].left

    @property
    def _right_raw(self) -> ibis.Table:
        return self._sublinkages[0].right

    @property
    def _links_raw(self) -> ibis.Table:
        raise NotImplementedError

    def cache(self) -> Linkage:
        """
        No-op to cache this, since the condition is an abstract condition, and
        left and right are already cached.
        """
        return self


class UnionLinkage(CombinedLinkage[L]):
    def __init__(self, sublinkages: Iterable[L]) -> None:
        flattened = []
        for sub in sublinkages:
            if isinstance(sub, UnionLinkage):
                flattened.extend(sub.__sublinkages__())
            else:
                flattened.append(sub)
        # TODO: I think we should be efficient here and do something like
        # union(
        #     linkages[0].links,
        #     <all the links in linkages[1] except add an additional join condition to
        #     not include links that are already in the first linkage>
        #     <all the links in linkages[2] except add an additional join condition to
        #     not include links that are already in the first or second linkage>
        #     <etc...>
        # )
        super().__init__(flattened)

    @property
    def _links_raw(self) -> ibis.Table:
        return ibis.union(*(sub.links for sub in self.__sublinkages__()))


class IntersectionLinkage(CombinedLinkage[L]):
    def __init__(self, sublinkages: Iterable[L]) -> None:
        flattened = []
        for sub in sublinkages:
            if isinstance(sub, IntersectionLinkage):
                flattened.extend(sub.__sublinkages__())
            else:
                flattened.append(sub)
        super().__init__(flattened)

    @property
    def _links_raw(self) -> ibis.Table:
        return ibis.intersect(*(sub.links for sub in self.__sublinkages__()))


class DifferenceLinkage(CombinedLinkage[L]):
    def __init__(self, sublinkages: Iterable[L]) -> None:
        # TODO: I think we can optimize this by flattening nested ones
        super().__init__(sublinkages)

    @property
    def _links_raw(self) -> ibis.Table:
        return ibis.difference(*(sub.links for sub in self.__sublinkages__()))


@runtime_checkable
class PCombiner(Protocol):
    def __call__(self, linkages: Iterable[Linkage]) -> Linkage: ...


class CombinerRegistry(Registry[PCombiner, Linkage]):
    def __call__(self, linkages: Iterable[Linkage]) -> Linkage:
        # Should we verify that all the linkages share the same left and right tables?
        linkages = tuple(linkages)
        non_linkages = [ln for ln in linkages if not isinstance(ln, Linkage)]
        if non_linkages:
            raise TypeError(f"received some non-Linkages: {non_linkages}", non_linkages)
        return super().__call__(linkages)


@runtime_checkable
class HasJoinConditionLinkage(HasJoinCondition, Protocol):
    pass


class AndJoinConditionsLinkage(CombinedLinkage[HasJoinConditionLinkage]):
    def __init__(self, sublinkages: Iterable[HasJoinConditionLinkage]):
        if not self.is_match(sublinkages):
            return NotImplemented
        flattened = []
        for sub in sublinkages:
            if isinstance(sub, AndJoinConditionsLinkage):
                flattened.extend(sub.__sublinkages__())
            else:
                flattened.append(sub)
        super().__init__(flattened)

    def __join_condition__(
        self, left: ibis.Table, right: ibis.Table
    ) -> Callable[[ibis.Table, ibis.Table], ibis.ir.BooleanValue]:
        return ibis.and_(
            *[sub.__join_condition__(left, right) for sub in self.__sublinkages__()]
        )

    @property
    def _links_raw(self) -> ibis.Table:
        return join(
            self._left_raw,
            self._right_raw,
            self.__join_condition__,
            lname="{name}_l",
            rname="{name}_r",
            rename_all=True,
        )

    @classmethod
    def is_match(cls, linkages: Iterable[Linkage]) -> bool:
        return all(isinstance(linkage, HasJoinConditionLinkage) for linkage in linkages)


union = CombinerRegistry()
"""Return a Linkage with links that are in any input Linkages."""
union.register(UnionLinkage)

intersect = CombinerRegistry()
"""Return a Linkage with links that are in all input Linkages."""
intersect.register(IntersectionLinkage)
intersect.register(AndJoinConditionsLinkage)

difference = CombinerRegistry()
"""
Return a Linkage with links that are in the first Linkage but not in any of the rest.
"""
difference.register(DifferenceLinkage)


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
        linkage.copy(links=linkage.links.select(*shared_columns))
        for linkage in linkages
    )
