from __future__ import annotations

import pytest


@pytest.fixture
def name_table(table_factory):
    names = [
        {
            "first": "JOHN",
            "last": "Doe",
            "suffix": "Jr.",
        },
        {
            "first": " john",
            "middle": "Q",
            "last": "Doe",
            "suffix": "Jr.",
        },
        {
            "prefix": "Ms.",
            "first": "Jane",
            "middle": "A",
            "last": "Smith",
            "nickname": "Janie",
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
