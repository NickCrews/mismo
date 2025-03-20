from __future__ import annotations

import logging
from typing import Callable, Generic, Iterable, TypeVar

logger = logging.getLogger(__name__)

Ret = TypeVar("Ret")
F = TypeVar("F", bound=Callable[..., Ret])


class Registry(Generic[F, Ret]):
    def __init__(
        self,
        implementations: Iterable[F] = (),
    ) -> None:
        self.implementations = tuple(implementations)
        """Mutable, so users can modify as needed."""

    def register(self, implementation: F) -> F:
        """
        Register (before existing implementations) a new implementation of the Combiner.
        """
        self.implementations = (implementation, *self.implementations)
        return implementation

    def __call__(self, *args, **kwargs) -> Ret:
        """
        Combine multiple Linkages by finding the first implementation that matches.
        """
        for implementation in self.implementations:
            try:
                return implementation(*args, **kwargs)
            except NotImplementedError:
                continue
        raise NotImplementedError
