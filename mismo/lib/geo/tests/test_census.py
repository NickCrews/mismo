from __future__ import annotations

import random

import pytest

from mismo.lib.geo import us_census_geocode
from mismo.tests.util import assert_tables_equal

DEFAULT_INS = {
    "id": None,
    "street": None,
    "city": None,
    "state": None,
    "zipcode": None,
}
SCHEMA_INS = {c: "string" for c in DEFAULT_INS}
DEFAULT_EXPECTED = {
    **DEFAULT_INS,
    "census_is_match": False,
    "census_match_type": None,
    "census_street": None,
    "census_city": None,
    "census_state": None,
    "census_zipcode": None,
    "census_latitude": None,
    "census_longitude": None,
}
SCHEMA_EXPECTED = {c: "string" for c in DEFAULT_EXPECTED}
SCHEMA_EXPECTED["census_is_match"] = "boolean"
SCHEMA_EXPECTED["census_latitude"] = "float64"
SCHEMA_EXPECTED["census_longitude"] = "float64"


def make_input(table_factory, records):
    records = [{**DEFAULT_INS, **record} for record in records]
    t = table_factory(records, schema=SCHEMA_INS)
    return t


def make_expected(table_factory, records):
    records = [{**DEFAULT_EXPECTED, **record} for record in records]
    t = table_factory(records, schema=SCHEMA_EXPECTED)
    return t


@pytest.mark.network
@pytest.mark.skip
def test_us_census_geocode(table_factory):
    GIRDWOOD = {
        "census_is_match": True,
        "census_match_type": "non_exact",
        "census_street": "285 HIGHER TER",
        "census_city": "GIRDWOOD",
        "census_state": "AK",
        "census_zipcode": "99587",
        "census_latitude": 60.954138,
        "census_longitude": -149.115947,
    }
    NORTH_43RD = {
        "census_is_match": True,
        "census_match_type": "exact",
        "census_street": "2114 N 43RD ST",
        "census_city": "SEATTLE",
        "census_state": "WA",
        "census_zipcode": "98103",
        "census_latitude": 47.659183,
        "census_longitude": -122.333537,
    }
    CASKI = {
        "census_is_match": True,
        "census_match_type": "non_exact",
        "census_street": "7258 CASKI CT",
        "census_city": "WASILLA",
        "census_state": "AK",
        "census_zipcode": "99654",
        "census_latitude": 61.497808,
        "census_longitude": -149.650161,
    }
    # not using pytest params because we want to make only one request
    # so the test is faster
    pairs = [
        (
            {"id": "43rd_basic", "street": "2114 North 43rd St", "zipcode": "98103"},
            NORTH_43RD,
        ),
        # can handle duplicates
        (
            {
                "id": "43rd_with_spaces",
                "street": "2114 NORTH 43RD ST  ",
                "zipcode": "98103",
            },
            NORTH_43RD,
        ),
        (
            {"id": "43rd_no_north", "street": "2114 43rd St", "zipcode": "98103"},
            {**NORTH_43RD, "census_match_type": "non_exact"},
        ),
        (
            # APTs are removed!
            {"id": "43rd_apt", "street": "2114 N 43rd St APT 3", "zipcode": "98103"},
            NORTH_43RD,
        ),
        (
            # doesn't exist
            {
                "id": "dne_elm",
                "street": "321   ELM ST",
                "city": "NEW YORK CITY",
                "state": "NEW YORK",
                "zipcode": "10001",
            },
            {"census_is_match": False},
        ),
        (
            {
                "id": "po_box",
                "street": "PO BOX 321",
                "city": "Girdwood",
                "state": "AK",
                "zipcode": "99587",
            },
            {"census_is_match": False},
        ),
        (
            {
                "id": "girdwood_typo_city",
                "street": "285 HIGHER Terrace road",
                "city": "GIRDWOOOD",  # typo
                "state": "AK",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "id": "girdwood_typo_city_serious",
                "street": "285 HIGHER Terrace road",
                "city": "schmirdwud",  # serious typo
                "state": "ALASKA",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "id": "girdwood_wrong_city",
                "street": "285 HIGHER Terrace road",
                "city": "seattle",  # totally misleading
                "state": "ALASKA",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "id": "girdwood_no_state_zip",
                "street": "285 HIGHER Terrace road",
                "city": "GIRDWOoOD",  # typo, no state or zip
            },
            GIRDWOOD,
        ),
        (
            {
                "id": "girdwood_no_state_zip_street_typo",
                "street": "285 HIGHhER Terrace road",  # typo
                "city": "GIRDWOOD",  # no state or zip
            },
            GIRDWOOD,
        ),
        # The lesson in the cases below is that if you aren't getting matches,
        # try REMOVING some info, because that info might be wrong.
        (
            {
                "id": "caski_wrong_zip_with_all",
                "street": "7258 S CASKI CIR",  # Actually is 7258 CASKI CT
                "city": "WASILLA",
                "state": "AK",
                "zipcode": "99623",  # wrong zip
            },
            {"census_is_match": False},
        ),
        (
            {
                "id": "caski_wrong_zip_without_street_type",
                "street": "7258 S CASKI",
                "city": "WASILLA",
                "state": "AK",
                "zipcode": "99623",  # wrong zip
            },
            {"census_is_match": False},
        ),
        (
            {
                "id": "caski_wrong_zip_without_directional",
                "street": "7258 CASKI CIR",  # Without the directional it works!
                "city": "WASILLA",
                "state": "AK",
                "zipcode": "99623",  # wrong zip
            },
            CASKI,
        ),
        (
            {
                "id": "caski",
                "street": "7258 S CASKI CIR",
                "city": "WASILLA",
                "state": "AK",
                "zipcode": "99654",  # with correct zip it works!
            },
            CASKI,
        ),
        (
            {
                "id": "caski_no_zip",
                "street": "7258 S CASKI CIR",
                "city": "WASILLA",
                "state": "AK",
                "zipcode": None,  # without the wrong zip it works!
            },
            CASKI,
        ),
    ]
    ins = [p[0] for p in pairs]
    exp = [{**p[0], **p[1]} for p in pairs]
    inp = make_input(table_factory, ins)
    expected = make_expected(table_factory, exp)
    geocoded = us_census_geocode(inp, chunk_size=2)
    assert_tables_equal(geocoded, expected, column_order="ignore")


@pytest.mark.network
def test_us_census_geocode_concurrent_chunks(table_factory):
    """E2E: geocode enough addresses to need several concurrent requests."""
    records = [
        {"id": str(i), "street": f"{2100 + i} N 43RD ST", "zipcode": "98103"}
        for i in range(20)
    ]
    inp = make_input(table_factory, records)
    geocoded = us_census_geocode(inp, chunk_size=5, n_concurrent=4)
    df = geocoded.to_pandas()
    assert len(df) == 20
    matched = df[df.census_is_match]
    # a real, geocodable block of N 43rd St: most house numbers should match
    assert len(matched) >= 10
    assert (matched.census_city == "SEATTLE").all()
    assert (matched.census_state == "WA").all()
    assert (matched.census_zipcode == "98103").all()
    assert matched.census_latitude.between(47, 48).all()
    assert matched.census_longitude.between(-123, -122).all()


@pytest.mark.network
def test_adaptive_concurrency_survives_server_overload():
    """E2E: deliberately exceed the census server's in-flight request cap.

    As of 2026-06 the server caps ~16 in-flight requests per client and
    rejects the excess with fast 502s (see bench_census.py for the data).
    With more concurrency than that, _make_requests should halve its limit
    on the 502s, retry the rejected chunks, and still complete every chunk,
    instead of burning all retries and raising like it used to.

    Slow (several minutes): sends 32 chunks x 1000 rows to the real API.
    """
    from mismo.lib.geo._census import _AdaptiveLimiter, _make_requests
    from mismo.lib.geo.bench_census import make_csv_bytes

    n_chunks = 32
    limiter = _AdaptiveLimiter(n_chunks)
    requests = [
        dict(bytes=make_csv_bytes(1000, salt=i), benchmark=None, vintage=None)
        for i in range(n_chunks)
    ]
    responses = _make_requests(requests, limiter=limiter)
    assert len(responses) == n_chunks
    assert all(isinstance(r, str) and r for r in responses)
    if limiter.limit == n_chunks:
        pytest.skip(
            "the server accepted all 32 concurrent requests, so the "
            "overload backoff path was never exercised this run"
        )
    assert limiter.limit < n_chunks


@pytest.mark.parametrize(
    "n",
    [
        10,
        100,
        1000,
    ],
)
@pytest.mark.network
def test_benchmark_us_census_geocode(benchmark, table_factory, n: int):
    records = [
        {"street": f"{random.randint(1, 1000)} N 43RD ST", "zipcode": "98103"}
        for _ in range(n)
    ]
    inp = make_input(table_factory, records)
    geocoded = benchmark(us_census_geocode, inp)
    assert geocoded.count().execute() == n
