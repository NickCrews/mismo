from __future__ import annotations

import importlib.metadata
import warnings

from mismo import arrays as arrays
from mismo import cluster as cluster
from mismo import compare as compare
from mismo import eda as eda
from mismo import exceptions as exceptions
from mismo import fs as fs
from mismo import joins as joins
from mismo import lib as lib
from mismo import linkage as linkage
from mismo import linker as linker
from mismo import playdata as playdata
from mismo import sets as sets
from mismo import text as text
from mismo import tf as tf
from mismo import types as types
from mismo import vector as vector
from mismo._counts_table import CountsTable as CountsTable
from mismo._datasets import Datasets as Datasets
from mismo._explain import explain as explain
from mismo._n_naive import n_naive_comparisons as n_naive_comparisons
from mismo._recipe import PRecipe as PRecipe
from mismo.joins import HasJoinCondition as HasJoinCondition
from mismo.joins import IntoHasJoinCondition as IntoHasJoinCondition
from mismo.joins import join as join
from mismo.joins import join_condition as join_condition
from mismo.joins import left as left
from mismo.joins import right as right
from mismo.linkage import Linkage as Linkage
from mismo.linker import EmptyLinker as EmptyLinker
from mismo.linker import FullLinker as FullLinker
from mismo.linker import IDLinker as IDLinker
from mismo.linker import JoinLinker as JoinLinker
from mismo.linker import KeyLinker as KeyLinker
from mismo.linker import Linker as Linker
from mismo.linker import OrLinker as OrLinker
from mismo.linker import empty_linkage as empty_linkage
from mismo.linker import full_linkage as full_linkage
from mismo.types import Diff as Diff
from mismo.types import DiffStats as DiffStats
from mismo.types import LinkCountsTable as LinkCountsTable
from mismo.types import LinkedTable as LinkedTable
from mismo.types import LinksTable as LinksTable
from mismo.types import UnionTable as UnionTable
from mismo.types import Updates as Updates

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError as e:
    warnings.warn(f"Could not determine version of {__name__}\n{e!s}", stacklevel=2)
    __version__ = "unknown"
