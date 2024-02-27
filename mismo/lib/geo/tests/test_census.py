from __future__ import annotations

import random

import pytest

from mismo.lib.geo import us_census_geocode
from mismo.tests.util import assert_tables_equal


def make_address_table(table_factory, records):
    default = {
        "street": None,
        "city": None,
        "state": None,
        "zipcode": None,
    }
    records = [{**default, **record, "id": i} for i, record in enumerate(records)]
    t = table_factory(
        records,
        schema={
            "id": "int64",
            "street": "string",
            "city": "string",
            "state": "string",
            "zipcode": "string",
        },
    )
    return t


def test_us_census_geocode(table_factory, monkeypatch):
    from mismo.lib.geo import _census

    # reduce the chunk size so we send multiple requests, to test that the
    # chunking actually works.
    monkeypatch.setattr(_census, "_CHUNK_SIZE", 5)

    GIRDWOOD = {
        "is_match": True,
        "match_type": "non_exact",
        "street": "285 HIGHER TER",
        "city": "GIRDWOOD",
        "state": "AK",
        "zipcode": "99587",
        "latitude": 60.954138,
        "longitude": -149.115947,
    }
    NORTH_43RD = {
        "is_match": True,
        "match_type": "exact",
        "street": "2114 N 43RD ST",
        "city": "SEATTLE",
        "state": "WA",
        "zipcode": "98103",
        "latitude": 47.659183,
        "longitude": -122.333537,
    }
    # not using pytest params because we want to make only one request
    # so the test is faster
    pairs = [
        (
            {"street": "2114 North 43rd St", "zipcode": "98103"},
            NORTH_43RD,
        ),
        (
            {"street": "2114 43rd St", "zipcode": "98103"},
            {**NORTH_43RD, "match_type": "non_exact"},
        ),
        (
            # APTs are removed!
            {"street": "2114 N 43rd St APT 3", "zipcode": "98103"},
            NORTH_43RD,
        ),
        (
            # doesn't exist
            {
                "street": "321   ELM ST",
                "city": "NEW YORK CITY",
                "state": "NEW YORK",
                "zipcode": "10001",
            },
            {"is_match": False},
        ),
        (
            {
                "street": "PO BOX 321",
                "city": "Girdwood",
                "state": "AK",
                "zipcode": "99587",
            },
            {"is_match": False},
        ),
        (
            {
                "street": "285 HIGHER Terrace road",
                "city": "GIRDWOOOD",  # typo
                "state": "AK",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "street": "285 HIGHER Terrace road",
                "city": "schmirdwud",  # serious typo
                "state": "ALASKA",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "street": "285 HIGHER Terrace road",
                "city": "seattle",  # totally misleading
                "state": "ALASKA",
                "zipcode": "99587",
            },
            GIRDWOOD,
        ),
        (
            {
                "street": "285 HIGHER Terrace road",
                "city": "GIRDWOoOD",  # typo, no state or zip
            },
            GIRDWOOD,
        ),
        (
            {
                "street": "285 HIGHhER Terrace road",  # typo
                "city": "GIRDWOOD",  # no state or zip
            },
            GIRDWOOD,
        ),
    ]
    ins, outs = zip(*pairs)
    inp = make_address_table(table_factory, ins)
    expected = make_address_table(table_factory, outs)
    geocoded = us_census_geocode(inp)
    # order columns
    geocoded = geocoded[[c for c in expected.columns]]
    assert_tables_equal(geocoded, expected, order_by="id")


@pytest.mark.parametrize(
    "n",
    [
        10,
        100,
        1000,
    ],
)
def test_benchmark_us_census_geocode(benchmark, table_factory, n: int):
    records = [
        {"street": f"{random.randint(1, 1000)} N 43RD ST", "zipcode": "98103"}
        for _ in range(n)
    ]
    inp = make_address_table(table_factory, records)
    geocoded = benchmark(us_census_geocode, inp)
    assert geocoded.count().execute() == n
