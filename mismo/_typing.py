from __future__ import annotations

from typing import TypeVar

# Little hack because mypy doesn't support
# Self types from either the stdlib or from typing_extensions
# https://github.com/python/mypy/issues/11871
Self = TypeVar("Self", bound="object")
