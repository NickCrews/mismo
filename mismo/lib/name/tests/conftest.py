from __future__ import annotations

import pytest


@pytest.fixture
def name_table(table_factory):
    names = [
        {
            "first": "Alice",
            "last": "Anderson",
        },
        {
            "first": "A",
            "last": "Anderson",
            "suffix": "PhD",
        },
        {
            "first": "Bob",
            "last": "Baker",
        },
        {
            "first": "Robert",
            "middle": "b",
            "last": "Baker",
            "suffix": "Jr.",
        },
        {
            "prefix": "Mr",
            "first": "Charles",
            "last": "Carter",
            "nickname": "Charlie",
        },
        {
            "first": "  CHARLES",
            "last": " CARTER.",
        },
    ]
    base = {
        "prefix": None,
        "first": None,
        "middle": None,
        "last": None,
        "suffix": None,
        "nickname": None,
    }
    names = [{**base, **name} for name in names]
    return table_factory({"name": names, "record_id": range(len(names))})
