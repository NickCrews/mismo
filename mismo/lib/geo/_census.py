from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
from typing import TYPE_CHECKING, Iterable

import ibis
from ibis.expr import types as ir

from mismo import _util

if TYPE_CHECKING:
    import httpx

# expose this here so we can monkeypatch it in tests
# The API supports max 10k addresses per request
_CHUNK_SIZE = 1_000
_N_CONCURRENT = 16
# These were re-tuned 2026-06-11 (previously chunk_size=5000), when the
# server had gotten ~8x slower than when first tuned: a serial 5k chunk took
# ~200s, past the 150s read timeout below, and 16 concurrent 5k chunks (80k
# rows in flight) made the server return 502s outright. 16 concurrent 1k
# chunks (16k rows in flight) all succeeded in 22-76s with the best
# throughput of any config tested (~210 rows/s).
# The server seems to choke on total in-flight rows, not connection count,
# and throughput scales ~linearly with concurrency up to ~16 in-flight
# requests per client; beyond that the excess requests just get 502s,
# so raising _N_CONCURRENT above 16 buys nothing.
# See bench_census.py (sibling to this file) for the methodology and data;
# rerun it if these settings start failing again.


logger = logging.getLogger(__name__)


class _AdaptiveLimiter:
    """A concurrency limiter whose limit adapts to server overload (AIMD).

    Like TCP congestion control: start optimistic at max_limit, halve the
    limit when the server signals overload (fast 502s, timeouts), and creep
    back up by 1 after a full round of successes at the current limit.
    The census server hard-caps in-flight requests per client (~16 as of
    2026-06, see bench_census.py) and rejects the excess with 502s rather
    than queueing. That cap and the server's speed both change over time,
    so we discover the workable concurrency at runtime instead of
    hardcoding it.
    """

    def __init__(self, max_limit: int) -> None:
        self.max_limit = max_limit
        self.limit = max_limit
        self._active = 0
        self._cond = asyncio.Condition()
        # Bumped on each backoff. A whole wave of in-flight requests fails
        # together when the server is overloaded, so each request records
        # the generation it started in, and only the first failure of a
        # generation halves the limit.
        self._generation = 0
        self._n_successes = 0

    async def acquire(self) -> int:
        """Wait for a slot, then return the current overload generation."""
        async with self._cond:
            await self._cond.wait_for(lambda: self._active < self.limit)
            self._active += 1
            return self._generation

    async def release(self) -> None:
        async with self._cond:
            self._active -= 1
            self._cond.notify_all()

    async def on_success(self) -> None:
        async with self._cond:
            self._n_successes += 1
            if self._n_successes >= self.limit and self.limit < self.max_limit:
                self._n_successes = 0
                self.limit += 1
                logger.info(
                    f"Census server healthy, raising concurrency to {self.limit}"
                )
                self._cond.notify_all()

    async def on_overload(self, generation: int) -> None:
        async with self._cond:
            if generation != self._generation:
                # Another failure from the same overload event already
                # backed off; don't halve again.
                return
            self._generation += 1
            self._n_successes = 0
            old = self.limit
            self.limit = max(1, self.limit // 2)
            logger.warning(
                f"Census server overloaded, reducing concurrency {old} -> {self.limit}"
            )


def us_census_geocode(
    t: ir.Table,
    format: str = "census_{name}",
    *,
    benchmark: str | None = None,
    vintage: str | None = None,
    chunk_size: int | None = None,
    n_concurrent: int | None = None,
) -> ir.Table:
    """Geocode US physical addresses using the US Census Bureau's geocoding service.

    Uses the batch geocoding API from https://geocoding.geo.census.gov/geocoder.
    This only works for US physical addresses. PO Boxes are not supported.
    "APT 123", "UNIT B", etc are not included in the results, so you will need
    to extract those before geocoding.

    Before geocoding, this function normalizes the input addresses and deduplicates
    them, so if your input table has 1M rows, but only 100k unique addresses,
    it will only send those 100k addresses to the API.

    This took about 7 minutes to geocode 1M unique addresses in my tests.

    Parameters
    ----------
    t:
        A table of addresses to geocode. Must have the schema:
        - street: string, the street address.
        - city: string, the city name.
        - state: string, the state name.
        - zipcode: string, the ZIP code.
    format:
        The format to use for the output column names. See the Returns section.
    benchmark:
        The geocoding benchmark to use. Default is "Public_AR_Current".
    vintage:
        The geocoding vintage to use. Default is "Current_Current".
    chunk_size:
        The number of addresses to geocode in each request. Default is 1000.
        The maximum allowed by the API is 10_000.
        This number was tuned experimentally, you probably don't need to change it.
    n_concurrent:
        The maximum number of concurrent requests to make. Default is 16.
        The actual concurrency adapts at runtime: it starts here and backs
        off when the server signals overload (502s, timeouts), then
        recovers as requests succeed.
        This number was tuned experimentally, you probably don't need to change it.

    Returns
    -------
    geocoded:
        The input table, with the following additional columns:
        - is_match: bool, whether the address was successfully matched.
          If False, all the other columns will be NULL.
        - match_type: string, the type of match. eg "exact", "non_exact"
        - street: string, the normalized street address
        - city: string, the normalized city name
        - state: string, the normalized 2 letter state code
        - zipcode: string, the 5 digit ZIP code
        - latitude: float64, the latitude of the matched address
        - longitude: float64, the longitude of the matched address
        Each of these columns is named according to the `format` parameter.
        For example, if `format` is "census_{name}", the columns will be named
        "census_is_match", "census_match_type", "census_street", etc.
        The order of the results is not guaranteed to match the input order.

    """
    if chunk_size is None:
        chunk_size = _CHUNK_SIZE
    if chunk_size > 10_000:
        raise ValueError("chunk_size must be <= 10_000")
    if n_concurrent is None:
        n_concurrent = _N_CONCURRENT
    t = t.mutate(__row_number=ibis.row_number())
    normed = _normed_addresses(t)
    deduped, restore = _dedupe(normed)
    gc = _geocode(
        deduped,
        benchmark=benchmark,
        vintage=vintage,
        chunk_size=chunk_size,
        n_concurrent=n_concurrent,
    )
    unduped = restore(gc)
    unduped = unduped.rename(
        {
            format.format(name=col): col
            for col in unduped.columns
            if col != "__row_number"
        }
    )
    input_cols = [c for c in t.columns if c != "__row_number"]
    geocode_cols = [c for c in unduped.columns if c != "__row_number"]
    re_joined = t.inner_join(unduped, "__row_number").select(*input_cols, *geocode_cols)
    return re_joined


def _dedupe(t: ir.Table) -> tuple[ir.Table, callable]:
    keys = ["street", "city", "state", "zipcode"]
    api_id = ibis.dense_rank().over(ibis.window(order_by=keys))
    # same as pandas.DataFrame.groupby(keys).ngroup()
    with_group_id = t.mutate(api_id=api_id)
    deduped = with_group_id.select(
        "api_id", "street", "city", "state", "zipcode"
    ).distinct()
    # need to cache this, otherwise if you look at deduped[:100], deduped[100:200],
    # etc, it will recompute deduped each time, and since the order is not guaranteed,
    # you might get api_id 1 both times!
    deduped = deduped.cache()
    restore_map = with_group_id.select("__row_number", "api_id")

    def restore(ded: ir.Table) -> ir.Table:
        return restore_map.left_join(ded, "api_id").drop("api_id", "api_id_right")

    return deduped, restore


def _geocode(
    t: ir.Table,
    *,
    benchmark,
    vintage,
    chunk_size: int,
    n_concurrent: int,
) -> ir.Table:
    t = t.cache()
    sub_tables = chunk_table(t, max_size=chunk_size)
    byte_chunks = (_table_to_csv_bytes(sub) for sub in sub_tables)
    limiter = _AdaptiveLimiter(n_concurrent)
    requests = [
        dict(bytes=b, benchmark=benchmark, vintage=vintage) for b in byte_chunks
    ]
    logger.debug(
        f"Geocoding {t.count().execute()} addresses in {len(requests)} chunks of size {chunk_size}"  # noqa
    )
    responses = _make_requests(requests, limiter=limiter)
    tables = (_text_to_table(resp_text) for resp_text in responses)
    result = ibis.union(*tables)
    result = _post_process_table(result)
    return result


def _normed_addresses(t: ir.Table) -> ir.Table:
    t = t.select(
        "__row_number",
        street=t.street.strip().upper(),
        city=t.city.strip().upper(),
        state=t.state.strip().upper(),
        zipcode=t.zipcode.strip().upper(),
    )
    return t


def chunk_table(t: ir.Table, max_size: int) -> Iterable[ir.Table]:
    n = t.count().execute()
    i = 0
    while i < n:
        yield t[i : i + max_size]
        i += max_size


def _table_to_csv_bytes(t: ir.Table) -> bytes:
    t = t.select("api_id", "street", "city", "state", "zipcode")
    df = t.to_pandas()
    return df.to_csv(index=False, header=False).encode("utf-8")


def _make_client() -> httpx.AsyncClient:
    with _util.optional_import("httpx"):
        import httpx
    timeout = httpx.Timeout(10, read=150, pool=10000)
    return httpx.AsyncClient(timeout=timeout)


def _make_requests(
    requests: Iterable[dict],
    *,
    limiter: _AdaptiveLimiter | None = None,
):
    if limiter is None:
        limiter = _AdaptiveLimiter(_N_CONCURRENT)

    async def _async_make_requests() -> list[str]:
        async with _make_client() as client:
            responses = [
                _make_request(client, limiter, chunk_id=i, **req)
                for i, req in enumerate(requests)
            ]
            return await asyncio.gather(*responses)

    return _asyncio_run_with_nest_asyncio(_async_make_requests())


# Status codes that mean "the server is overloaded", as opposed to
# "something is wrong with this particular request". As of 2026-06 the
# census gateway returns a fast 502 for each request beyond its in-flight
# cap, rather than queueing it. See bench_census.py.
_OVERLOAD_STATUS_CODES = (429, 502, 503, 504)


async def _make_request(
    client: httpx.AsyncClient,
    limiter: _AdaptiveLimiter,
    *,
    chunk_id: int,
    bytes: bytes,
    benchmark: str | None = None,
    vintage: str | None = None,
) -> str:
    with _util.optional_import("httpx"):
        import httpx

    URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
    data = {
        "benchmark": benchmark or "Public_AR_Current",
        "vintage": vintage or "Current_Current",
    }
    files = {"addressFile": ("addresses.csv", bytes)}

    async def f():
        generation = await limiter.acquire()
        logger.debug(f"Geocoding chunk {chunk_id}")
        start = time.monotonic()
        try:
            resp = await client.post(URL, data=data, files=files)
        except httpx.TimeoutException:
            # An overloaded server sometimes accepts a request and then
            # never answers it (see bench_census.py), so a timeout is an
            # overload signal too.
            await limiter.on_overload(generation)
            raise
        finally:
            await limiter.release()
        if resp.status_code in _OVERLOAD_STATUS_CODES:
            await limiter.on_overload(generation)
            raise RuntimeError(
                f"Census server overloaded on chunk {chunk_id}: HTTP {resp.status_code}"
            )
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to geocode chunk {chunk_id}: {resp.text}")
        await limiter.on_success()
        end = time.monotonic()
        logger.debug(f"Geocoded chunk {chunk_id} in {end - start:.2f} seconds")
        return resp.text

    return await _with_retries(f, chunk_id=chunk_id, max_retries=3)


async def _with_retries(f, *, chunk_id, max_retries: int):
    for i in range(max_retries):
        try:
            return await f()
        except Exception as e:
            logger.warning(
                f"Retrying chunk {chunk_id} {i + 1}/{max_retries} after error: {e!r}"
            )
            if i == max_retries - 1:
                raise RuntimeError(f"Failed to geocode chunk {chunk_id}: {e!r}") from e
            else:
                # exponential backoff: 15, 30, 60, ... seconds.
                # The server works on timescales of 10-100s per request
                # (see bench_census.py), so sub-second backoff would retry
                # straight into the same overload window. The main throttle
                # is re-acquiring the limiter at its reduced limit; this
                # sleep just keeps fast-rejected chunks from spinning.
                await asyncio.sleep(15 * 2**i)


def _text_to_table(text: str) -> ir.Table:
    _RAW_SCHEMA = ibis.schema(
        {
            "api_id": "string",
            "address": "string",
            "match": "string",
            "matchtype": "string",
            "parsed": "string",
            "coordinate": "string",
            "tigerlineid": "string",
            "side": "string",
        }
    )
    with io.StringIO(text) as f:
        records = list(csv.DictReader(f, fieldnames=list(_RAW_SCHEMA.keys())))
    if not records:
        records = {col: [] for col in _RAW_SCHEMA}
    return ibis.memtable(records, schema=_RAW_SCHEMA)


def _post_process_table(t: ir.Table) -> ir.Table:
    lonlat = t.coordinate.split(",")
    lon, lat = lonlat[0].cast("float64"), lonlat[1].cast("float64")
    # pattern is r"(.*), (.*), (.*), (.*)" but splitting on ", " is (probably?) faster
    parts: ir.ArrayColumn = t.parsed.split(", ")
    street = parts[0].strip()
    city = parts[1].strip()
    state = parts[2].strip()
    zipcode = parts[3].strip()
    return t.select(
        api_id=t.api_id.cast("int64"),
        is_match=t.match == "Match",
        match_type=t.matchtype.lower(),
        street=street,
        city=city,
        state=state,
        zipcode=zipcode,
        latitude=lat,
        longitude=lon,
    )


def _asyncio_run_with_nest_asyncio(coro):
    """asyncio.run(), but can handle nested loops as in Jupyter."""
    try:
        return asyncio.run(coro)
    except RuntimeError as e:
        if "asyncio.run() cannot be called from a running event loop" not in str(e):
            raise
        import mismo._nest_asyncio as nest_asyncio

        nest_asyncio.apply()
        return asyncio.run(coro)
