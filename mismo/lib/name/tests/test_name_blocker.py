from __future__ import annotations

import pytest

from mismo.lib.name import NameBlocker


def test_name_blocker_with_column(name_table):
    blocker = NameBlocker(column="name")
    linkage = blocker(name_table, name_table)
    record_ids = linkage.links.select("record_id_l", "record_id_r").execute()
    record_ids = set(record_ids.itertuples(index=False, name=None))
    expected = {(1, 2), (3, 4), (5, 6)}
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
