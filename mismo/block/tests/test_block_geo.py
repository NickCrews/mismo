from __future__ import annotations

from ibis import _
import pytest

from mismo.block import CoordinateBlocker, block


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
    t1 = table_factory({"coord": [{"lat": lat1, "lon": lon1}]})
    t2 = table_factory({"coord": [{"lat": lat2, "lon": lon2}]})
    blocker = CoordinateBlocker(distance_km=km, **kwargs)
    blocked = block(t1, t2, blocker)
    n_blocked = blocked.count().execute()
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
