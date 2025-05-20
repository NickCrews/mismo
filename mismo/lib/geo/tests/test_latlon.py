from __future__ import annotations

import ibis
from ibis import _
import numpy as np
import pytest

from mismo.lib.geo import CoordinateBlocker, distance_km


@pytest.mark.parametrize(
    "lat1, lon1, lat2, lon2, expected",
    [
        (0, 0, 0, 0, 0),
        (None, 0, 0, 0, np.nan),
        (0, 0, 0, 180, 20015.086796020572),
        (0, 0, 0, 90, 10007.543398010286),
    ],
)
def test_distance_km(lat1, lon1, lat2, lon2, expected):
    lat1 = ibis.literal(lat1, type="double")
    lon1 = ibis.literal(lon1, type="double")
    lat2 = ibis.literal(lat2, type="double")
    lon2 = ibis.literal(lon2, type="double")
    result = distance_km(lat1=lat1, lon1=lon1, lat2=lat2, lon2=lon2).execute()
    assert result == pytest.approx(expected) or np.isnan(result) and np.isnan(expected)


@pytest.mark.parametrize(
    "coord1,coord2,km,expected",
    [
        ((0, 0), (0, 0), 1, True),
        pytest.param(
            (61.1547800, -150.0677490),
            (61.1582056, -150.0584552),
            1,
            True,
            id=".6km-anchorage-1km",
        ),
        pytest.param(
            (61.1547800, -150.0677490),
            (61.1582056, -150.0584552),
            0.1,
            False,
            id=".6km-anchorage-100m",
        ),
        # tests for https://github.com/NickCrews/mismo/issues/74
        pytest.param((0, 0), (0, 0.5), 1, False, id="50km-equator-1km"),
        pytest.param((0, 0), (0, 0.5), 100, True, id="50km-equator-100km"),
    ],
)
@pytest.mark.parametrize(
    "kwargs",
    [
        {"coord": "coord"},
        {"left_coord": "coord", "right_coord": _.coord},
        {"left_coord": "coord", "right_lat": _.coord.lat, "right_lon": _.coord.lon},
        {"lat": _.coord.lat, "lon": _.coord.lon},
        {
            "left_lat": _.coord.lat,
            "left_lon": _.coord.lon,
            "right_lat": _.coord.lat,
            "right_lon": _.coord.lon,
        },
    ],
)
def test_coordinate_blocker(table_factory, coord1, coord2, km, expected, kwargs):
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    t1 = table_factory(
        {
            "coord": [{"lat": lat1, "lon": lon1}],
            "record_id": [42],
        }
    )
    t2 = table_factory(
        {
            "coord": [{"lat": lat2, "lon": lon2}],
            "record_id": [53],
        }
    )
    blocker = CoordinateBlocker(distance_km=km, **kwargs)
    linkage = blocker(t1, t2)
    n_blocked = linkage.links.count().execute()
    if expected:
        assert n_blocked == 1
    else:
        assert n_blocked == 0


@pytest.mark.parametrize(
    "kwarg_names",
    [
        {"coord", "left_coord"},
        {"coord", "lat"},
        {"coord", "lat", "lon"},
        {"lat", "right_lat"},
        {"left_coord", "right_coord", "left_lat"},
    ],
)
def test_coordinate_blocker_error(kwarg_names):
    kwargs = {name: "x" for name in kwarg_names}
    with pytest.raises(ValueError):
        CoordinateBlocker(distance_km=1, **kwargs)
