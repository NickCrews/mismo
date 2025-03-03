from __future__ import annotations

import asyncio
from contextlib import closing, contextmanager
from typing import AsyncIterable, Iterable, Iterator, TypeVar

T = TypeVar("T")


def iter_over_async(
    ait: AsyncIterable[T], loop: asyncio.AbstractEventLoop | None = None
) -> Iterable[T]:
    """Iterate over an async iterator in a synchronous manner.

    Either pass in a loop, or make sure there is an event loop running when
    calling this function.
    """
    # copied from https://stackoverflow.com/a/63595496/5156887
    ait = ait.__aiter__()
    DONE = object()

    async def get_next():
        try:
            return await ait.__anext__()
        except StopAsyncIteration:
            return DONE

    with with_event_loop(loop) as loop:
        while True:
            nxt = get_next()
            result = run_until_complete(nxt, loop)
            if result is DONE:
                break
            yield result


def run_until_complete(future, loop: asyncio.AbstractEventLoop):
    if not loop.is_running():
        return loop.run_until_complete(future)
    import nest_asyncio

    nest_asyncio.apply()
    return loop.run_until_complete(future)


@contextmanager
def with_event_loop(
    loop: asyncio.AbstractEventLoop | None = None,
) -> Iterator[asyncio.AbstractEventLoop]:
    if loop is not None:
        yield loop
    else:
        try:
            # This works if a loop is already running, eg we are in a Jupyter notebook
            yield asyncio.get_running_loop()
        except RuntimeError:
            # Otherwise, create a new loop, and close it when done
            with closing(asyncio.new_event_loop()) as loop:
                yield loop
