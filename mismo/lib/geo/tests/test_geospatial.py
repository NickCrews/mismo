from __future__ import annotations

import ibis
import numpy as np
import pytest

from mismo.lib.geo import distance_km


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
