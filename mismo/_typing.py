from __future__ import annotations

try:
    from typing import Self as Self
except ImportError:
    from typing_extensions import Self as Self


def get_annotations(o):
    """Access o.__annotations__ across python versions."""
    # from https://docs.python.org/3/howto/annotations.html
    # TODO: once we drop support for python 3.9, we can remove this function
    # and just use inspect.get_annotations directly
    try:
        from inspect import get_annotations

        return get_annotations(o)
    except ImportError:
        pass

    if isinstance(o, type):
        return o.__dict__.get("__annotations__", {})
    else:
        return getattr(o, "__annotations__", {})
