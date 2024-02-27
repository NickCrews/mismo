from __future__ import annotations

import asyncio
from typing import (
    Any,
    AsyncIterator,
    Coroutine,
    Iterable,
)


def as_completed(
    coroutines: Iterable[Coroutine], *, loop: asyncio.AbstractEventLoop | None = None
) -> Iterable[Any]:
    """Run coroutines concurrently and yield results as they come in."""

    async def loop():
        for result in asyncio.as_completed(coroutines):
            yield await result

    yield from _iter_over_async(loop())


def _iter_over_async(ait: AsyncIterator, loop: asyncio.AbstractEventLoop | None = None):
    if loop is None:
        loop = asyncio.get_event_loop()
    ait = ait.__aiter__()
    DONE = object()

    async def get_next():
        try:
            return await ait.__anext__()
        except StopAsyncIteration:
            return DONE

    while True:
        obj = loop.run_until_complete(get_next())
        if obj is DONE:
            break
        yield obj
