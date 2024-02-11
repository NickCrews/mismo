from __future__ import annotations

import pytest

from mismo.block import _geo, block


@pytest.mark.parametrize(
    "coord1,coord2,km,expected",
    [
        ((0, 0), (0, 0), 1, True),
        pytest.param(
            (61.1547800, -150.0677490),
            (61.1582056, -150.0584552),
            1,
            True,
            id=".6km in anchorage",
        ),
        pytest.param(
            (61.1547800, -150.0677490),
            (61.1582056, -150.0584552),
            0.1,
            False,
            id=".6km in anchorage",
        ),
    ],
)
def test_bin_lat_lon(table_factory, coord1, coord2, km, expected):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    t1 = table_factory({"coord": [{"lat": lat1, "lon": lon1}]})
    t2 = table_factory({"coord": [{"lat": lat2, "lon": lon2}]})
    blocker = _geo.CoordinateBlocker(km, "coord", "coord")
    blocked = block(t1, t2, blocker)
    n_blocked = blocked.count().execute()
    if expected:
        assert n_blocked == 1
    else:
        assert n_blocked == 0
