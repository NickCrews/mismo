from __future__ import annotations

import pytest


@pytest.fixture
def name_table(table_factory):
    records = [
        {
            "record_id": "<null>",
            "name": None,
        },
        {
            "record_id": "alice_anderson",
            "name": {
                "given": "Alice",
                "surname": "Anderson",
            },
        },
        {
            "record_id": "a_anderson_phd",
            "name": {
                "given": "A",
                "surname": "Anderson",
                "suffix": "PhD",
            },
        },
        {
            "record_id": "bob_baker",
            "name": {
                "given": "Bob",
                "surname": "Baker",
            },
        },
        {
            "record_id": "robert_b_baker_jr",
            "name": {
                "given": "Robert",
                "middle": "b",
                "surname": "Baker",
                "suffix": "Jr.",
            },
        },
        {
            "record_id": "mr_charles_carter",
            "name": {
                "prefix": "Mr",
                "given": "Charles",
                "surname": "Carter",
                "nickname": "Charlie",
            },
        },
        {
            "record_id": "charles_carter",
            "name": {
                "given": "  CHARLES",
                "surname": " CARTER.",
            },
        },
    ]
    base = {
        "prefix": None,
        "given": None,
        "middle": None,
        "surname": None,
        "suffix": None,
        "nickname": None,
    }
    records = [
        {
            **r,
            "name": {**base, **(r["name"])} if r["name"] is not None else None,
        }
        for r in records
    ]
    return table_factory(records)
