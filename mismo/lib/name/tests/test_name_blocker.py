from __future__ import annotations

import pytest

from mismo.lib.name import NameBlocker


def test_name_blocker_with_column(name_table):
    blocker = NameBlocker(column="name")
    linkage = blocker(name_table, name_table)
    record_ids = linkage.links.select("record_id_l", "record_id_r").execute()
    record_ids = set(
        [tuple(pair) for pair in record_ids.itertuples(index=False, name=None)]
    )
    expected = {
        ("alice_anderson", "a_anderson_phd"),
        ("bob_baker", "robert_b_baker_jr"),
        ("charles_carter", "mr_charles_carter"),
    }
    expected = set((a, b) if a < b else (b, a) for a, b in expected)
    assert record_ids == expected


def test_name_blocker_with_invalid_arguments():
    with pytest.raises(ValueError):
        NameBlocker()
    with pytest.raises(ValueError):
        NameBlocker(column="name", column_left="name", column_right="name")
        NameBlocker(column="name", column_left="name")
    with pytest.raises(ValueError):
        NameBlocker(column_left="name")
    with pytest.raises(ValueError):
        NameBlocker(column_right="name")
