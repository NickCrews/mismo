from __future__ import annotations

import ibis


def check_tables_and_links(
    left: ibis.Table, right: ibis.Table, links: ibis.Table
) -> None:
    if "record_id" not in left.columns:
        raise ValueError("column 'record_id' not in table")
    if "record_id" not in right.columns:
        raise ValueError("column 'record_id' not in other")
    if "record_id_l" not in links.columns:
        raise ValueError("column 'record_id_l' not in links")
    if "record_id_r" not in links.columns:
        raise ValueError("column 'record_id_r' not in links")
    try:
        left.record_id == links.record_id_l
    except Exception:
        raise ValueError(
            f"left.record_id of type {left.record_id.type()} is not comparable with links.record_id_l of type {links.record_id_l.type()}"  # noqa: E501
        )
    try:
        right.record_id == links.record_id_r
    except Exception:
        raise ValueError(
            f"right.record_id of type {right.record_id.type()} is not comparable with links.record_id_r of type {links.record_id_r.type()}"  # noqa: E501
        )
