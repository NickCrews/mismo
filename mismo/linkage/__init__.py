from __future__ import annotations

from mismo.linkage._combine import difference as difference
from mismo.linkage._combine import intersect as intersect
from mismo.linkage._combine import (
    unify_links_min_intersection as unify_links_min_intersection,
)
from mismo.linkage._combine import union as union
from mismo.linkage._key_linkage_combined import UnionKeyLinkage as UnionKeyLinkage
from mismo.linkage._key_linkage_combined import (
    register_combiners as _register_combiners,
)
from mismo.linkage._key_linker import KeyLinkage as KeyLinkage
from mismo.linkage._key_linker import KeyLinker as KeyLinker
from mismo.linkage._label_linker import LabelLinker as LabelLinker
from mismo.linkage._linkage import BaseLinkage as BaseLinkage
from mismo.linkage._linkage import Linkage as Linkage
from mismo.linkage._linkage import LinkTableLinkage as LinkTableLinkage
from mismo.linkage._linkage import filter_links as filter_links

_register_combiners()
