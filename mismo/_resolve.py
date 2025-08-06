from __future__ import annotations

import operator
from typing import Any, Callable, Generator, Protocol, runtime_checkable

import ibis
from ibis import Deferred
from ibis.common.deferred import BinaryOperator, Resolver, Variable
from ibis.expr import types as ir

from mismo import _funcs, _util


@runtime_checkable
class ValueResolver(Protocol):
    """A callable, that given a Table, resolves to a single column."""

    def __call__(self, t: ibis.Table) -> ibis.Column:
        """Given a Table, resolve to a single column."""


class DeferredResolver(ValueResolver):
    def __init__(
        self,
        deferred: ibis.Deferred,
        name: str | None = None,
    ) -> None:
        if name is None:
            name = "_"
        self.deferred = deferred
        self.name = name

    def __call__(self, t: ibis.Table) -> ibis.Column:
        raw = self.deferred.resolve(**{self.name: t})
        return _resolve(t, raw)

    def __repr__(self) -> str:
        if self.name == "_":
            return f"DeferredResolver({self.deferred!r})"
        return f"DeferredResolver({self.deferred!r}, name={self.name!r})"

    def __str__(self) -> str:
        return f"{self.deferred!r}"


class LiteralResolver(ValueResolver):
    def __init__(self, value: ibis.Value) -> None:
        self.value = value

    def __call__(self, t: ibis.Table) -> ibis.Column:
        """Resolve a literal value."""
        resolved = t.bind(self.value)
        if len(resolved) != 1:
            raise ValueError(
                f"Expected 1 column, got {len(resolved)} from {self.value}"
            )
        return resolved[0]

    def __repr__(self) -> str:
        return f"LiteralResolver({self.value!r})"

    def __str__(self) -> str:
        return f"`{self.value!r}`"


class StrResolver(ValueResolver):
    def __init__(
        self,
        s: str,
    ) -> None:
        self.s = s

    def __call__(self, t: ibis.Table) -> ibis.Column:
        """Resolve a string to a column."""
        return t[self.s]

    def __repr__(self):
        return f"StrResolver({self.s!r})"

    def __str__(self):
        return f"`{self.s!r}`"


class FuncResolver(ValueResolver):
    def __init__(
        self,
        func: Callable[[ibis.Table], ibis.Column],
    ) -> None:
        self.func = func

    def __call__(self, t: ibis.Table) -> ibis.Column:
        return self.func(t)

    def __repr__(self):
        return f"FuncResolver({self.func!r})"


def _resolve(t: ir.Table, spec) -> ir.Value:
    values = t.bind(spec)
    if len(values) != 1:
        raise ValueError(f"Expected 1 column, got {len(values)} from {spec}")
    return values[0]


def value_resolver(spec: ibis.Value | Deferred | str) -> ValueResolver:
    """
    Given a spec, return a ValueResolver that resolves to a single column.
    """
    if isinstance(spec, ibis.Value):
        return LiteralResolver(spec)
    if isinstance(spec, ibis.Deferred):
        if variables_names(spec) == {"_"}:
            return DeferredResolver(spec, "_")
        raise ValueError(f"Deferred objects must represent a single column, got {spec}")
    if isinstance(spec, str):
        return StrResolver(spec)
    if _funcs.is_unary(spec):
        return FuncResolver(spec)
    raise ValueError(f"Cannot resolve {spec} to a single column.")


def key_pair_resolver(spec) -> tuple[ValueResolver, ValueResolver]:
    if isinstance(spec, ibis.Value):
        return (value_resolver(spec), value_resolver(spec))
    if isinstance(spec, ibis.Deferred):
        if variables_names(spec) == {"_"}:
            return (value_resolver(spec), value_resolver(spec))
        raise ValueError(f"Deferred objects must represent a single column, got {spec}")
    if isinstance(spec, str):
        return (value_resolver(spec), value_resolver(spec))
    if _funcs.is_unary(spec):
        return (value_resolver(spec), value_resolver(spec))
    parts = _util.promote_list(spec)
    if len(parts) != 2:
        raise ValueError(
            (
                "Column spec, when an iterable, must be length 2.",
                f" got {len(parts)} from {spec}",
            )
        )
    keyl, keyr = parts
    if isinstance(keyl, Deferred):
        left_resolver = DeferredResolver(keyl, _get_name(keyl, {"left", "_"}))
    else:
        left_resolver = value_resolver(keyl)
    if isinstance(keyr, Deferred):
        right_resolver = DeferredResolver(keyr, _get_name(keyr, {"right", "_"}))
    else:
        right_resolver = value_resolver(keyr)

    return (left_resolver, right_resolver)


def _get_name(d: Deferred, allowed_names: set[str]) -> str:
    """Get the name of a Deferred, or raise an error if it is not in allowed_names."""
    actual_names = variables_names(d)
    if len(actual_names) != 1:
        raise ValueError(
            f"Expected a Deferred with name in {allowed_names}, got {actual_names} from {d}"  # noqa: E501
        )
    actual_name = actual_names.pop()
    if actual_name not in allowed_names:
        raise ValueError(
            f"Expected a Deferred with name in {allowed_names}, got {actual_name} from {d}"  # noqa: E501
        )
    return actual_name


def key_pair_resolvers(x) -> list[tuple[ValueResolver, ValueResolver]]:
    """
    Given a spec or iterable of specs, return a list of KeyPairResolvers.
    """
    if (deferred_resolvers := _parse_and_of_equals(x)) is not None:
        return deferred_resolvers
    return [key_pair_resolver(spec) for spec in _util.promote_list(x)]


def _parse_and_of_equals(x) -> list[tuple[DeferredResolver, DeferredResolver]] | None:
    """Look for the special case of an `and` of equalities, like `left.col1 == right.col2 & left.col3 == right.col4 & ...`"""  # noqa: E501
    if not isinstance(x, ibis.Deferred):
        return None
    if variables_names(x) != {"left", "right"}:
        return None
    resolver = _resolver(x)
    return list(_traverse_deferred_resolvers(resolver))


class BadDeferredError(ValueError):
    pass


def _traverse_deferred_resolvers(
    resolver: Resolver,
) -> Generator[tuple[DeferredResolver, DeferredResolver], None, None]:
    if not isinstance(resolver, BinaryOperator):
        return
    if resolver.func == operator.and_:
        # Recurse on the left and right operands
        yield from _traverse_deferred_resolvers(resolver.left)
        yield from _traverse_deferred_resolvers(resolver.right)
    elif resolver.func == operator.eq:
        if variables_names(resolver.left) == {"left"} and variables_names(
            resolver.right
        ) == {"right"}:
            left_resolver = resolver.left
            right_resolver = resolver.right
        elif variables_names(resolver.left) == {"right"} and variables_names(
            resolver.right
        ) == {"left"}:
            left_resolver = resolver.right
            right_resolver = resolver.left
        else:
            raise BadDeferredError(
                f"Expected an equality between left and right, got {resolver}"
            )
        yield (
            DeferredResolver(Deferred(left_resolver), name="left"),
            DeferredResolver(Deferred(right_resolver), name="right"),
        )
    else:
        raise BadDeferredError(
            "Expected 1-N equality conditions combined with `and`"
            "(eg `left.col1 == right.col2 & left.col3 == right.col4 & ...`), "
            f"got {resolver.func} instead."
        )


def _resolver(x: Any) -> Resolver | None:
    """Get the resolver from an object, if it has one."""
    if isinstance(x, Resolver):
        return x
    if hasattr(x, "_resolver"):
        return x._resolver
    return None


def variables_names(deferred: Deferred | Resolver) -> set[str]:
    """Get the names of all Variables in a Deferred.

    Examples
    --------
    >>> import ibis
    >>> import mismo

    The builtin deferred variable `ibis._` has the name "_".

    >>> sorted(variables_names(ibis._.foo + ibis._.fill_null(5)))
    ['_']

    The `mismo.left` and `mismo.right` variables have the names "left" and "right".

    >>> sorted(variables_names(mismo.left.foo + mismo.right.bar + ibis._.baz))
    ['_', 'left', 'right']

    We don't really use this anywhere in mismo, but just so you see how it works,
    you can create your own deferred variables with names.

    >>> from ibis.common.deferred import var
    >>> sorted(variables_names(ibis._.foo + var("bar").baz.fill_null(8)))
    ['_', 'bar']
    """
    resolver = _resolver(deferred)
    if not isinstance(resolver, Resolver):
        raise TypeError(
            f"Expected a Deferred or Resolver, got {type(deferred).__name__} instead."
        )
    names = set()
    for resolver in _traverse_resolver(resolver):
        if isinstance(resolver, Variable):
            names.add(resolver.name)
    return names


def _traverse_resolver(x: Any) -> Generator[Resolver, None, None]:
    if isinstance(x, Resolver):
        yield x
    if hasattr(x, "__slots__"):
        for slot in x.__slots__:
            child = getattr(x, slot)
            yield from _traverse_resolver(child)
