from __future__ import annotations

import asyncio
import csv
import io
import logging
from typing import TYPE_CHECKING, Iterable

import ibis
from ibis.expr import types as it

from mismo import _aio, _util

if TYPE_CHECKING:
    import httpx

# expose this here so we can monkeypatch it in tests
# The API supports 10k addresses per request, but
# we'll use a smaller number to avoid timeouts, and for smaller
# total sizes well get better parallelism (eg 10k rows will go
# in 2 small chunks instead of 1)
_CHUNK_SIZE = 5_000

logger = logging.getLogger(__name__)


def us_census_geocode(
    t: it.Table,
    *,
    benchmark: str | None = None,
    vintage: str | None = None,
    chunk_size: int | None = None,
) -> it.Table:
    """Geocode US physical addresses using the US Census Bureau's geocoding service.

    Uses the batch geocoding API from https://geocoding.geo.census.gov/geocoder.
    This only works for US physical addresses. PO Boxes are not supported.
    "APT 123", "UNIT B", etc are not included in the results, so you will need
    to extract those before geocoding.

    Parameters
    ----------
    t:
        A table of addresses to geocode. Must have the schema:
        - id: string or int, anything unique
        - street: string, the street address.
        - city: string, the city name.
        - state: string, the state name.
        - zipcode: string, the ZIP code.
    benchmark:
        The geocoding benchmark to use. Default is "Public_AR_Current".
    vintage:
        The geocoding vintage to use. Default is "Current_Current".
    chunk_size:
        The number of addresses to geocode in each request. Default is 5000.

    Returns
    -------
    geocoded:
        A table with the following schema:
        - id: same as input
        - is_match: bool, whether the address was successfully matched
        - match_type: string, the type of match. eg "exact", "non_exact"
        - street: string, the normalized street address (if matched)
        - city: string, the normalized city name (if matched)
        - state: string, the normalized 2 letter state code (if matched)
        - zipcode: string, the 5 digit ZIP code (if matched)
        - latitude: float64, the latitude of the matched address
        - longitude: float64, the longitude of the matched address
        The order of the results is not guaranteed to match the input order.
        Use the id column to join the results back to the input table.

    """
    if chunk_size is None:
        chunk_size = _CHUNK_SIZE
    id_type = t.id.type()
    t = _prep_address(t)
    sub_tables = chunk_table(t, max_size=chunk_size)
    byte_chunks = (_table_to_csv_bytes(sub) for sub in sub_tables)
    client = _make_client()
    sem = asyncio.Semaphore(50)
    tasks = [
        _make_request(client, b, sem, i, benchmark=benchmark, vintage=vintage)
        for i, b in enumerate(byte_chunks)
    ]
    logger.debug(f"Geocoding {len(tasks)} chunks of addresses of size {_CHUNK_SIZE}")
    text_responses = _aio.as_completed(tasks)
    tables = (_text_to_table(resp_text) for resp_text in text_responses)
    t = ibis.union(*tables)
    t = _post_process_table(t, id_type)
    return t


def _prep_address(t: it.Table) -> it.Table:
    t = t.select(
        id=t.id.cast("string"),
        street=t.street.strip(),
        city=t.city.strip(),
        state=t.state.strip(),
        zipcode=t.zipcode.strip(),
    )
    return t


def chunk_table(t: it.Table, max_size: int) -> Iterable[it.Table]:
    t = t.cache()
    n = t.count().execute()
    i = 0
    while i < n:
        yield t[i : i + max_size]
        i += max_size


def _table_to_csv_bytes(t: it.Table) -> bytes:
    df = t.to_pandas()
    return df.to_csv(index=False, header=False).encode("utf-8")


def _make_client() -> httpx.AsyncClient:
    with _util.optional_import("httpx"):
        import httpx
    timeout = httpx.Timeout(5, read=100, pool=10000)
    limits = httpx.Limits(max_keepalive_connections=None, max_connections=1000)
    return httpx.AsyncClient(timeout=timeout, limits=limits)


async def _make_request(
    client: httpx.AsyncClient,
    b: bytes,
    sem: asyncio.Semaphore,
    chunk_id: int,
    *,
    benchmark: str | None = None,
    vintage: str | None = None,
) -> str:
    URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
    data = {
        "benchmark": benchmark or "Public_AR_Current",
        "vintage": vintage or "Current_Current",
    }
    files = {"addressFile": ("addresses.csv", b)}

    async def f():
        async with sem:
            logger.debug(f"Geocoding chunk {chunk_id}")
            resp = await client.post(URL, data=data, files=files)
            resp.raise_for_status()
            logger.debug(f"Geocoded chunk {chunk_id}")
            return resp.text

    return await _with_retries(f, chunk_id=chunk_id, max_retries=3)


async def _with_retries(f, *, chunk_id, max_retries: int):
    for i in range(max_retries):
        try:
            return await f()
        except Exception as e:
            logger.info(
                f"Retrying chunk {chunk_id} {i + 1}/{max_retries} after error: {e!r}"
            )
            if i == max_retries - 1:
                raise RuntimeError(f"Failed to geocode chunk {chunk_id}: {e!r}") from e
            else:
                # exponential backoff
                # 0.5, 1, 2, 4, 8
                await asyncio.sleep(0.5 * 2**i)


def _text_to_table(text: str) -> it.Table:
    _RAW_SCHEMA = ibis.schema(
        {
            "id": "string",
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


def _post_process_table(t: it.Table, id_type) -> it.Table:
    lonlat = t.coordinate.split(",")
    lon, lat = lonlat[0].cast("float64"), lonlat[1].cast("float64")
    # pattern is r"(.*), (.*), (.*), (.*)" but splitting on ", " is (probably?) faster
    parts: it.ArrayColumn = t.parsed.split(", ")
    street = parts[0].strip()
    city = parts[1].strip()
    state = parts[2].strip()
    zipcode = parts[3].strip()
    return t.select(
        id=t.id.cast(id_type),
        is_match=t.match == "Match",
        match_type=t.matchtype.lower(),
        street=street,
        city=city,
        state=state,
        zipcode=zipcode,
        latitude=lat,
        longitude=lon,
    )
