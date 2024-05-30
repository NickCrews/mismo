from __future__ import annotations

import asyncio
from typing import AsyncIterator, Iterable, TypeVar

T = TypeVar("T")


def iter_over_async(
    ait: AsyncIterator[T], loop: asyncio.AbstractEventLoop | None = None
) -> Iterable[T]:
    """Iterate over an async iterator in a synchronous manner.

    Either pass in a loop, or make sure there is an event loop running when
    calling this function.
    """
    # copied from https://stackoverflow.com/a/63595496/5156887
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
        nxt = get_next()
        try:
            obj = loop.run_until_complete(nxt)
        except RuntimeError as e:
            if "This event loop is already running" not in str(e):
                raise
            import nest_asyncio

            nest_asyncio.apply()
            obj = loop.run_until_complete(nxt)
        if obj is DONE:
            break
        yield obj
