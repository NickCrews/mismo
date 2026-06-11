# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx"]
# ///
"""Benchmark the US Census batch geocoder to find what triggers 502s and timeouts.

The production code in _census.py sends chunks of 5000 addresses with 16
concurrent requests and a 150s read timeout. When the API starts returning
502s or timing out, we want to know which knob is to blame:

- chunk size: are single big requests slow/failing even with no concurrency?
- concurrency: do requests that succeed serially start failing when we send
  many at once?
- service flakiness: do even tiny serial requests fail right now?

Run with plain `python bench_census.py` (httpx is the only dependency).
Use --help to see how to run a subset of experiments or change sizes.

Experiments (each request uses distinct, deterministic synthetic addresses so
server-side caching can't skew results):

A. serial chunk-size sweep: one request at a time, increasing chunk size.
B. concurrency sweep at production chunk size (5000).
C. high concurrency at small chunk size (1000), to separate "too many
   connections" from "too many total in-flight rows".

The read timeout defaults to 300s (production uses 150s) so we can observe
whether big chunks *complete* in 150-300s rather than just "timed out".
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
import statistics
import time
from dataclasses import dataclass

import httpx

URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"

# Real streets so a realistic fraction of generated addresses match.
_SEEDS = [
    ("PENNSYLVANIA AVE NW", "WASHINGTON", "DC", "20500"),
    ("BROAD ST", "SEATTLE", "WA", "98109"),
    ("W NORTHERN LIGHTS BLVD", "ANCHORAGE", "AK", "99503"),
    ("MINNESOTA DR", "ANCHORAGE", "AK", "99503"),
    ("MAIN ST", "BOISE", "ID", "83702"),
    ("CONGRESS AVE", "AUSTIN", "TX", "78701"),
    ("MICHIGAN AVE", "CHICAGO", "IL", "60611"),
    ("PEACHTREE ST NE", "ATLANTA", "GA", "30303"),
    ("MARKET ST", "SAN FRANCISCO", "CA", "94103"),
    ("BROADWAY", "NEW YORK", "NY", "10012"),
]


def make_csv_bytes(n: int, *, salt: int) -> bytes:
    """n distinct addresses as the id,street,city,state,zip CSV the API wants.

    `salt` keeps addresses distinct across requests so the server can't serve
    a request faster because it just geocoded the same rows.
    """
    lines = []
    for i in range(n):
        street, city, state, zipcode = _SEEDS[i % len(_SEEDS)]
        house = 100 + ((salt * 7919 + i) % 9000)
        lines.append(f"{i},{house} {street},{city},{state},{zipcode}")
    return ("\n".join(lines) + "\n").encode("utf-8")


@dataclass
class Result:
    experiment: str
    chunk_size: int
    concurrency: int
    elapsed: float
    outcome: str  # "ok", "http <code>", "ReadTimeout", "ConnectTimeout", ...
    n_result_rows: int = 0


async def post_chunk(
    client: httpx.AsyncClient,
    *,
    experiment: str,
    chunk_size: int,
    concurrency: int,
    salt: int,
) -> Result:
    body = make_csv_bytes(chunk_size, salt=salt)
    data = {"benchmark": "Public_AR_Current", "vintage": "Current_Current"}
    files = {"addressFile": ("addresses.csv", body)}
    start = time.monotonic()
    try:
        resp = await client.post(URL, data=data, files=files)
        elapsed = time.monotonic() - start
        if resp.status_code == 200:
            outcome = "ok"
            n_rows = resp.text.count("\n")
        else:
            outcome = f"http {resp.status_code}"
            n_rows = 0
    except Exception as e:
        elapsed = time.monotonic() - start
        outcome = type(e).__name__
        n_rows = 0
    result = Result(experiment, chunk_size, concurrency, elapsed, outcome, n_rows)
    print(
        f"  [{experiment}] chunk={chunk_size} conc={concurrency} "
        f"-> {outcome} in {elapsed:.1f}s ({n_rows} rows)",
        flush=True,
    )
    return result


async def run_wave(
    client: httpx.AsyncClient,
    *,
    experiment: str,
    chunk_size: int,
    concurrency: int,
    salt_counter: itertools.count,
) -> list[Result]:
    """Fire `concurrency` simultaneous requests of `chunk_size` addresses each."""
    tasks = [
        post_chunk(
            client,
            experiment=experiment,
            chunk_size=chunk_size,
            concurrency=concurrency,
            salt=next(salt_counter),
        )
        for _ in range(concurrency)
    ]
    return list(await asyncio.gather(*tasks))


def summarize(results: list[Result]) -> None:
    print("\n=== Summary (grouped by experiment/chunk_size/concurrency) ===")
    header = f"{'experiment':<12}{'chunk':>7}{'conc':>6}{'ok':>7}{'latency s (min/med/max of OK)':>32}  errors"
    print(header)
    print("-" * len(header))
    keyfunc = lambda r: (r.experiment, r.chunk_size, r.concurrency)  # noqa: E731
    for key, group in itertools.groupby(sorted(results, key=keyfunc), key=keyfunc):
        group = list(group)
        ok = [r for r in group if r.outcome == "ok"]
        errors = sorted({r.outcome for r in group if r.outcome != "ok"})
        if ok:
            lats = [r.elapsed for r in ok]
            lat = f"{min(lats):.0f} / {statistics.median(lats):.0f} / {max(lats):.0f}"
        else:
            lat = "-"
        exp, chunk, conc = key
        print(
            f"{exp:<12}{chunk:>7}{conc:>6}{len(ok):>4}/{len(group):<3}{lat:>31}  {', '.join(errors)}"
        )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--experiments",
        default="a,b,c",
        help="comma-separated subset of a (serial chunk sizes), "
        "b (concurrency at chunk 5000), c (concurrency 16 at chunk 1000)",
    )
    parser.add_argument(
        "--chunk-sizes",
        default="10,500,1000,2500,5000",
        help="chunk sizes for experiment a",
    )
    parser.add_argument(
        "--concurrencies",
        default="4,8,16",
        help="concurrency levels for experiment b",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=300,
        help="read timeout in seconds (production uses 150)",
    )
    args = parser.parse_args()
    experiments = set(args.experiments.split(","))
    chunk_sizes = [int(s) for s in args.chunk_sizes.split(",")]
    concurrencies = [int(s) for s in args.concurrencies.split(",")]

    timeout = httpx.Timeout(10, read=args.read_timeout, pool=10_000)
    results: list[Result] = []
    salt_counter = itertools.count(1)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if "a" in experiments:
            print("\n--- Experiment A: serial requests, increasing chunk size ---")
            for size in chunk_sizes:
                results += await run_wave(
                    client,
                    experiment="a:serial",
                    chunk_size=size,
                    concurrency=1,
                    salt_counter=salt_counter,
                )
        if "b" in experiments:
            print("\n--- Experiment B: concurrency sweep at chunk size 5000 ---")
            for conc in concurrencies:
                results += await run_wave(
                    client,
                    experiment="b:conc",
                    chunk_size=5000,
                    concurrency=conc,
                    salt_counter=salt_counter,
                )
        if "c" in experiments:
            print("\n--- Experiment C: concurrency 16 at small chunk size 1000 ---")
            results += await run_wave(
                client,
                experiment="c:conc-small",
                chunk_size=1000,
                concurrency=16,
                salt_counter=salt_counter,
            )
    summarize(results)


if __name__ == "__main__":
    asyncio.run(main())
