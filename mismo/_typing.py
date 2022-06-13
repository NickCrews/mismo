import sys
from typing import TypeVar

import pandas as pd

if sys.version_info < (3, 8):
    from typing_extensions import Protocol as Protocol  # noqa: F401
else:
    from typing import Protocol as Protocol  # noqa: F401


Links = pd.DataFrame  # 2xN array of indices, an edge list
# 3xN array. First 2 columns are Links. Third column is 0 or 1, that edge exists
LabeledLinks = pd.DataFrame
Data = pd.DataFrame
Features = pd.DataFrame
Scores = pd.Series
ClusterIds = pd.Series

# Little hack because mypy doesn't support
# Self types from either the stdlib or from typing_extensions
# https://github.com/python/mypy/issues/11871
Self = TypeVar("Self", bound="object")
