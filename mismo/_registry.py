from __future__ import annotations

import logging
from typing import Callable, Generic, Iterable, TypeVar

logger = logging.getLogger(__name__)

Ret = TypeVar("Ret")
F = TypeVar("F", bound=Callable[..., Ret])


class Registry(Generic[F, Ret]):
    """A registry for different implementations, similar to functools.singledispatch.

    An implementation can be registered with the `register` method.

    When the registry is called, it will try each registered implementation in order.
    Each implementation is a callable that can either:
    - Return the sentinel value `NotImplemented` if it cannot handle the input,
      signaling that the next implementation should be tried.
    - Return anything else, which will be returned as the result.

    Examples
    --------
    >>> from mismo._registry import Registry
    >>> import json
    >>> import datetime
    >>> to_json = Registry[Callable[..., str], str]()
    >>> @to_json.register
    ... def _default(obj: object) -> str:
    ...     try:
    ...         return json.dumps(obj)
    ...     except TypeError:
    ...         # If the object is not JSON serializable,
    ...         # fall back to one of the registered implementations.
    ...         return NotImplemented
    >>> @to_json.register
    ... def _datetime(dt: datetime.datetime) -> str:
    ...     return dt.isoformat()
    >>> @to_json.register
    ... def _date(date: datetime.date) -> str:
    ...     return date.isoformat()
    >>> to_json(datetime.datetime(2023, 1, 1, 12, 0, 0))
    '2023-01-01T12:00:00'
    >>> to_json(datetime.date(2023, 1, 1))
    '2023-01-01'
    >>> to_json({"key": 4})
    '{"key": 4}'
    """

    def __init__(
        self,
        implementations: Iterable[F] = (),
    ) -> None:
        self.implementations = tuple(implementations)
        """Mutable, so users can modify as needed."""

    def register(self, implementation: F) -> F:
        """
        Register (after existing implementations) a new implementation of the Registry.
        """
        self.implementations = (*self.implementations, implementation)
        return implementation

    def __call__(self, *args, **kwargs) -> Ret:
        """
        Combine multiple Linkages by finding the first implementation that matches.
        """
        for implementation in self.implementations:
            result = implementation(*args, **kwargs)
            if result is NotImplemented:
                continue
            else:
                return result
        raise NotImplementedError
