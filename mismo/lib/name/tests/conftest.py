from __future__ import annotations

import pytest


@pytest.fixture
def name_table(table_factory):
    names = [
        None,
        {
            "given": "Alice",
            "surname": "Anderson",
        },
        {
            "given": "A",
            "surname": "Anderson",
            "suffix": "PhD",
        },
        {
            "given": "Bob",
            "surname": "Baker",
        },
        {
            "given": "Robert",
            "middle": "b",
            "surname": "Baker",
            "suffix": "Jr.",
        },
        {
            "prefix": "Mr",
            "given": "Charles",
            "surname": "Carter",
            "nickname": "Charlie",
        },
        {
            "given": "  CHARLES",
            "surname": " CARTER.",
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
    names = [{**base, **name} if name is not None else None for name in names]
    return table_factory({"name": names, "record_id": range(len(names))})
