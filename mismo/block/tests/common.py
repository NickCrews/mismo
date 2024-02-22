from __future__ import annotations


def letter_blocked_ids(table_factory):
    """If you block the fixtures t1 and t2 on the letter column,
    these are the record_ids what you should get."""
    return table_factory(
        {
            "record_id_l": [1, 2],
            "record_id_r": [90, 91],
        }
    )
