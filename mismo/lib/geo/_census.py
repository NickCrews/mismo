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
_CHUNK_SIZE = 5_000
_N_CONCURRENT = 16
# If I use a chunk size of 2k,
# with 50 concurrent connections, each request takes 20-30 seconds
# with 100 concurrent connections, each request takes 60-8 seconds
# This implies to me that there are limited resources on the server.
# If you make more requests, the same number of cores are serving them,
# so each request takes longer. But not just linearly,
# probably because of the overhead of context switching.

# results:
# 2k chunk size, 25 concurrent requests: ~15s/req, 5.25 min for 500k rows
# 4k chunk size, 25 concurrent requests: ~30s/req, 5 min for 500k rows
# 4k chunk size, 15 concurrent requests: ~22s/req, 3.75 min for 500k rows
# 5k chunk size, 15 concurrent requests: ~??s/req, 3.25 min for 500k rows
# 5k chunk size, 15 concurrent requests: ~26s/req, 3.5 min for 500k rows
# 10k chunk size, 15 concurrent requests: ~65s/req, 4 min for 500k rows
# 5k chunk size, 10 concurrent requests: ~25s/req, 4.5 min for 500k rows
# 5k chunk size, 20 concurrent requests: ~35s/req, 4.5 min for 500k rows
# 5k chunk size, 16 concurrent requests: ~30s/req, 3.5 min for 500k rows


logger = logging.getLogger(__name__)


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
        The number of addresses to geocode in each request. Default is 5000.
        The maximum allowed by the API is 10_000.
        This number was tuned experimentally, you probably don't need to change ir.
    n_concurrent:
        The number of concurrent requests to make. Default is 16.
        This number was tuned experimentally, you probably don't need to change ir.

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
    re_joined = t.inner_join(unduped, "__row_number").drop("__row_number")
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
    client = _make_client()
    sem = asyncio.Semaphore(n_concurrent)
    requests = [
        dict(bytes=b, benchmark=benchmark, vintage=vintage) for b in byte_chunks
    ]
    logger.debug(
        f"Geocoding {t.count().execute()} addresses in {len(requests)} chunks of size {chunk_size}"  # noqa
    )
    responses = _make_requests(requests, client=client, sem=sem)
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
    client: httpx.AsyncClient | None = None,
    sem: asyncio.Semaphore | None = None,
):
    if client is None:
        client = _make_client()
    if sem is None:
        sem = asyncio.Semaphore(_N_CONCURRENT)

    async def _async_make_requests() -> list[str]:
        async with client:
            responses = [
                _make_request(client, sem, chunk_id=i, **req)
                for i, req in enumerate(requests)
            ]
            return await asyncio.gather(*responses)

    return _asyncio_run_with_nest_asyncio(_async_make_requests())


async def _make_request(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    *,
    chunk_id: int,
    bytes: bytes,
    benchmark: str | None = None,
    vintage: str | None = None,
) -> str:
    URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
    data = {
        "benchmark": benchmark or "Public_AR_Current",
        "vintage": vintage or "Current_Current",
    }
    files = {"addressFile": ("addresses.csv", bytes)}

    # Sometimes I get a 502 error:
    # While attempting to geocode your batch input, the Census Geocoder
    # working as a gateway to get a response needed to handle the request,
    # got an invalid response. This may be caused by the fact that the
    # Census Geocoder is unable to connect to a service it is dependent
    # response received was invalid. Please verify your batch file and
    # resubmit your batch query again later.

    # Then if I resubmit the same request, it works. So I don't think the issue
    # is the format of the request. I think the Census Geocoder is just flaky.
    # Nothing to do but retry.

    async def f():
        async with sem:
            logger.debug(f"Geocoding chunk {chunk_id}")
            start = time.monotonic()
            resp = await client.post(URL, data=data, files=files)
            if resp.status_code != 200:
                raise RuntimeError(f"Failed to geocode chunk {chunk_id}: {resp.text}")
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
                # exponential backoff
                # 0.5, 1, 2, 4, 8
                await asyncio.sleep(0.5 * 2**i)


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
        import nest_asyncio

        nest_asyncio.apply()
        return asyncio.run(coro)
