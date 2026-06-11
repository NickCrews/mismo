# /// script
# requires-python = ">=3.10"
# dependencies = ["httpx"]
# ///
"""Benchmark the US Census batch geocoder to find what triggers 502s and timeouts.

Run with plain `python bench_census.py` (httpx is the only dependency).
The full default run takes ~25 minutes and sends ~150k addresses; use
--experiments to run a subset (see --help).

WHY THIS EXISTS (investigation of 2026-06-11)
=============================================
Production (_census.py) sent chunks of 5000 addresses with 16 concurrent
requests and a 150s read timeout. One day nearly 100% of requests started
failing with ReadTimeout or HTTP 502. The suspects:

- our internet connection (slow wifi)?
- chunk size: are single big requests slow/failing even with no concurrency?
- concurrency: do requests that succeed serially fail when sent together?
- service flakiness: do even tiny serial requests fail?

Each experiment below isolates one suspect, and its banner records what we
found on 2026-06-11 so a future run can be compared against that baseline.
The bottom line back then:

1. The server had gotten ~8x slower than when _census.py was tuned: a serial
   5000-address chunk took ~200s (vs ~26s historically), past the 150s read
   timeout. Every production request was dying even though the server would
   eventually have answered most of them.
2. 16 concurrent 5000-row requests (80k rows in flight) overloaded the
   server outright: half the wave came back 502 / timed out at 300s.
3. It was NOT the network: the same 206KB payload uploaded to a neutral
   endpoint in ~2s. ~98% of census request time was server-side processing.
4. It was NOT the number of connections: 16 concurrent 1000-row requests
   (16k rows in flight) all succeeded, faster than any other config tested.
5. Server throughput was NOT fixed: per-request latency stayed roughly
   constant as concurrency rose, so total throughput scaled almost linearly
   with concurrency -- but only up to ~16 in-flight requests. At concurrency
   32 (chunk 1000), exactly 16 requests succeeded and the rest got fast 502s
   or hung, so the server seems to hard-cap ~16 in-flight requests per
   client and reject the excess rather than queue it. Concurrency beyond 16
   buys nothing (160 rows/s at conc 32 vs 210 at conc 16).
6. Server speed varies a lot between runs: the same serial 5000-row request
   took 199s and then 87s about an hour apart. Compare configs within one
   run, not across runs.

The fix: chunk_size 5000 -> 1000, keeping 16 concurrent requests.

Each request uses distinct, deterministic synthetic addresses so server-side
caching can't skew results. The read timeout defaults to 300s (production
used 150s) so we can observe whether big chunks *complete* in 150-300s
rather than just "timed out".
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
import itertools
import statistics
import sys
import time

import httpx

URL = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
# Any fast endpoint that accepts a multipart POST works as the network control.
CONTROL_URL = "https://httpbin.org/post"

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


async def run_network_control(client: httpx.AsyncClient, *, chunk_size: int) -> None:
    """Experiment NET: is the bottleneck our connection or the server?

    Upload the identical multipart payload to a neutral fast endpoint and to
    the census API. The neutral upload time is (almost) pure network; the gap
    between the two is server-side processing. For the census request we also
    record time-to-response-headers separately from time-to-body-complete, to
    show the wait is before the first byte (server thinking), not during
    download.
    """
    body = make_csv_bytes(chunk_size, salt=0)
    files = {"addressFile": ("addresses.csv", body)}
    print(f"  payload: {chunk_size} addresses, {len(body):,} bytes")

    start = time.monotonic()
    try:
        resp = await client.post(CONTROL_URL, files=files)
        control_s = time.monotonic() - start
        print(
            f"  neutral endpoint ({CONTROL_URL}): "
            f"http {resp.status_code} in {control_s:.1f}s "
            f"-> your connection moves this payload in seconds"
        )
    except Exception as e:
        control_s = time.monotonic() - start
        print(
            f"  neutral endpoint ({CONTROL_URL}): {type(e).__name__} "
            f"after {control_s:.1f}s -> network problem on YOUR end, "
            f"census results below are not trustworthy"
        )

    data = {"benchmark": "Public_AR_Current", "vintage": "Current_Current"}
    start = time.monotonic()
    try:
        async with client.stream("POST", URL, data=data, files=files) as resp:
            headers_s = time.monotonic() - start
            await resp.aread()
            total_s = time.monotonic() - start
        print(
            f"  census endpoint: http {resp.status_code}, "
            f"response headers after {headers_s:.1f}s, body after {total_s:.1f}s"
        )
        print(
            f"  -> upload+download account for ~{control_s:.1f}s; "
            f"the remaining ~{headers_s - control_s:.0f}s is census "
            f"server-side processing"
        )
    except Exception as e:
        total_s = time.monotonic() - start
        print(f"  census endpoint: {type(e).__name__} after {total_s:.1f}s")


def summarize(results: list[Result]) -> None:
    print("\n=== Summary (grouped by experiment/chunk_size/concurrency) ===")
    print(
        "Each group is one wave of simultaneous requests, so rows/s is\n"
        "(rows geocoded in the group) / (slowest request in the group).\n"
    )
    header = (
        f"{'experiment':<12}{'chunk':>7}{'conc':>6}{'ok':>7}"
        f"{'latency s (min/med/max of OK)':>32}{'rows/s':>8}  errors"
    )
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
            throughput = f"{sum(r.n_result_rows for r in ok) / max(lats):.0f}"
        else:
            lat = "-"
            throughput = "-"
        exp, chunk, conc = key
        print(
            f"{exp:<12}{chunk:>7}{conc:>6}{len(ok):>4}/{len(group):<3}"
            f"{lat:>31}{throughput:>8}  {', '.join(errors)}"
        )
    print(
        "\nHow to read this (what we concluded on 2026-06-11):\n"
        "- If experiment NET showed the neutral upload is fast but census is\n"
        "  slow, the bottleneck is the census server, not your network.\n"
        "- If serial latency (a) grows superlinearly with chunk size and\n"
        "  exceeds the production read timeout, big chunks are unusable no\n"
        "  matter what concurrency is.\n"
        "- If per-request latency in (b)/(d) holds roughly constant as\n"
        "  concurrency rises, throughput scales ~linearly with concurrency --\n"
        "  until the overload cliff, which shows up as 502s/timeouts rather\n"
        "  than graceful slowdown.\n"
        "- Compare (b) conc=16/chunk=5000 against (c)+(d) at chunk 1000: if\n"
        "  the former fails and the latter succeed, the trigger is total\n"
        "  in-flight rows, not the number of connections.\n"
        "- Pick the config with the best rows/s whose max latency stays\n"
        "  comfortably under the production read timeout."
    )


_EXPERIMENTS_HELP = """\
comma-separated subset of:
  net: upload the same payload to a neutral endpoint vs census, to separate
       network time from server time. (2026-06-11: 206KB took 2s to httpbin
       vs 158s to census -> ~98%% of the time was census server processing.)
  a:   serial requests, increasing chunk size. (2026-06-11: 10->3s, 500->8s,
       1000->58s, 2500->150s, 5000->199s. Superlinear in rows, not bytes ->
       server compute. 5k chunks exceeded the production 150s timeout even
       with zero concurrency.)
  b:   concurrency sweep at production chunk size 5000. (2026-06-11: conc 4
       and 8 each ran at ~serial latency -> throughput scaled ~linearly.
       conc 16 = 80k rows in flight: 6/16 got 502 at ~100s, 2/16 timed out
       at 300s -> overload cliff, not slowdown.)
  c:   concurrency 16 at chunk 1000. (2026-06-11: 16/16 ok in 22-76s, best
       throughput tested -> 502s are about in-flight rows, not connections.)
  d:   higher concurrency at chunk 1000, to find where the cliff is for
       small chunks and whether throughput keeps scaling. (2026-06-11: at
       conc 32, exactly 16 succeeded in 78-100s, 14 got 502 within 31-49s,
       2 timed out at 300s -> the server caps ~16 in-flight requests per
       client. 160 rows/s, WORSE than conc 16's 210 rows/s.)
"""


async def main() -> None:
    # so progress is visible live even when output is piped to a file
    sys.stdout.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--experiments", default="net,a,b,c,d", help=_EXPERIMENTS_HELP)
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
        "--d-concurrencies",
        default="32",
        help="concurrency levels for experiment d (chunk 1000); experiments "
        "a and c provide the conc=1 and conc=16 points of the same curve",
    )
    parser.add_argument(
        "--read-timeout",
        type=float,
        default=300,
        help="read timeout in seconds (production used 150)",
    )
    args = parser.parse_args()
    experiments = set(args.experiments.split(","))
    chunk_sizes = [int(s) for s in args.chunk_sizes.split(",")]
    concurrencies = [int(s) for s in args.concurrencies.split(",")]
    d_concurrencies = [int(s) for s in args.d_concurrencies.split(",")]

    timeout = httpx.Timeout(10, read=args.read_timeout, pool=10_000)
    results: list[Result] = []
    salt_counter = itertools.count(1)
    async with httpx.AsyncClient(timeout=timeout) as client:
        if "net" in experiments:
            print("\n--- Experiment NET: your network vs the census server ---")
            await run_network_control(client, chunk_size=5000)
        if "a" in experiments:
            print("\n--- Experiment A: serial requests, increasing chunk size ---")
            print("    (how does latency scale with chunk size, no concurrency?)")
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
            print("    (do big chunks that succeed serially fail when concurrent?)")
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
            print("    (same connection count as production: rows or connections?)")
            results += await run_wave(
                client,
                experiment="c:conc-small",
                chunk_size=1000,
                concurrency=16,
                salt_counter=salt_counter,
            )
        if "d" in experiments:
            print("\n--- Experiment D: higher concurrency at chunk size 1000 ---")
            print("    (does throughput keep scaling, and where is the cliff?)")
            for conc in d_concurrencies:
                results += await run_wave(
                    client,
                    experiment="d:conc-small",
                    chunk_size=1000,
                    concurrency=conc,
                    salt_counter=salt_counter,
                )
    summarize(results)


if __name__ == "__main__":
    asyncio.run(main())
