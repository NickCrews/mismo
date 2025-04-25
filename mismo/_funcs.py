from __future__ import annotations

import inspect


def is_unary(f) -> bool:
    """Check if f is usable as f(x)

    This means that:
    - f is callable
    - f has at least one parameter, which is not keyword-only
    - all other parameters have default values
    """
    if not callable(f):
        return False
    params = inspect.signature(f).parameters
    if len(params) == 0:
        return False
    first, *rest = params.values()
    if first.kind not in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.VAR_POSITIONAL,
    ):
        return False
    if _need_value(rest):
        return False
    return True


def is_binary(f) -> bool:
    """Check if f is usable as f(x, y)

    This means that:
    - f is callable
    - f has at least 2 parameters, neither of which is keyword-only
    - all other parameters have default values
    """
    if not callable(f):
        return False
    params = inspect.signature(f).parameters
    if len(params) < 1:
        return False
    if len(params) == 1:
        first, *rest = params.values()
        if first.kind is not inspect.Parameter.VAR_POSITIONAL:
            return False
    else:
        first, second, *rest = params.values()
        if first.kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        ):
            if second.kind not in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.VAR_POSITIONAL,
            ):
                return False
        elif first.kind == inspect.Parameter.VAR_POSITIONAL:
            pass
        else:
            return False
    if _need_value(rest):
        return False
    return True


def _need_value(params: list[inspect.Parameter]) -> bool:
    """Check if any of the parameters need a value when called."""
    for param in params:
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            continue
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            continue
        if param.default == inspect.Parameter.empty:
            return True
    return False
