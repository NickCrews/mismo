import sys
from typing import TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import Protocol as Protocol  # noqa: F401
else:
    from typing import Protocol as Protocol  # noqa: F401

# Little hack because mypy doesn't support
# Self types from either the stdlib or from typing_extensions
# https://github.com/python/mypy/issues/11871
Self = TypeVar("Self", bound="object")
