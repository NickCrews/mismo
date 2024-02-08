from __future__ import annotations


def letters_blocked(table_factory):
    """If you block the fixtures t1 and t2 on the letters column,
    this is what you should get."""
    return table_factory(
        {
            "record_id_l": [1, 2],
            "record_id_r": [90, 91],
            "letters_l": ["b", "c"],
            "letters_r": ["b", "c"],
        }
    )
