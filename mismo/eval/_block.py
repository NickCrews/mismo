from __future__ import annotations

from typing import Sized, Union

Sizable = Union[int, Sized]


def _get_len(x: Sizable) -> int:
    if isinstance(x, int):
        return x
    else:
        return len(x)


def reduction_ratio(datal: Sizable, datar: Sizable, links: Sizable) -> float:
    naive_links = (_get_len(datal) * _get_len(datar)) / 2
    return 1 - _get_len(links) / naive_links
