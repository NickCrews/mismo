from __future__ import annotations

import dataclasses
import math
from typing import Callable

import ibis
from ibis.common.deferred import Deferred
from ibis.expr import types as it


@dataclasses.dataclass
class CoordinateBlocker:
    """Blocks two locations together if they are within a certain distance."""

    distance_km: float | int
    """The (approx) max distance in kilometers that two coords will be blocked together.
    
    This isn't precise, and can include pairs that are actually up to about 2x
    larger than this distance.
    This is because we use a simple grid to bin the coordinates, so
    1. This isn't accurate near the poles, and
    2. This isn't accurate near the international date line (longitude 180/-180).
    3. If two coords fall within opposite corners of the same grid cell, they
       will be blocked together even if they are further apart than the
       precision, due to the diagonal distance being longer than the horizontal
       or vertical distance.
    """

    left_col: str | Deferred | Callable[[it.Table], it.StructColumn]
    """The column from the left table containing the (lat, lon) coordinates."""

    right_col: str | Deferred | Callable[[it.Table], it.StructColumn]
    """The column from the right table containing the (lat, lon) coordinates."""

    def __call__(
        self,
        left: it.Table,
        right: it.Table,
        **kwargs,
    ) -> it.Table:
        """Return a hash value for the two coordinates."""
        left_col = left._ensure_expr(self.left_col)
        right_col = right._ensure_expr(self.right_col)
        # We have to use a grid size of ~3x the precision to avoid
        # two points falling right on either side of a grid cell boundary
        grid_size = self.distance_km * 3
        left_hashed = _bin_lat_lon(left_col.lat, left_col.lon, grid_size)
        right_hashed = _bin_lat_lon(right_col.lat, right_col.lon, grid_size)
        return left_hashed == right_hashed


def _bin_lat_lon(
    lat: it.FloatingValue, lon: it.FloatingValue, grid_size_km: float | int
) -> it.StructValue:
    """Bin a latitude or longitude to a grid of a given precision.

    Say you have two coordinates, (lat1, lon1) and (lat2, lon2), and you
    want to see if they are within, say 15km of each other. You can bin
    them both to a grid of 15km precision and compare the resulting
    hashed values. If they are the same, the coordinates are within 15km
    of each other.
    """
    km_per_lat, km_per_lon = _km_per_degree(lat)
    step_size_lat = grid_size_km / km_per_lat
    step_size_lon = grid_size_km / km_per_lon

    result = ibis.struct(
        {
            "lat_hash": (lat // step_size_lat),
            "lon_hash": (lon // step_size_lon),
        }
    )
    both_null = lat.isnull() & lon.isnull()
    return both_null.ifelse(ibis.null(), result)


def _km_per_degree(lat: it.FloatingValue) -> tuple[it.FloatingValue, float]:
    # Radius of the Earth in kilometers
    R = 6371.0
    lat_rad = lat * (math.pi / 180)
    # This is a constant value at any given latitude
    km_per_lat = (math.pi * R) / 180
    # This varies based on the latitude
    km_per_lon = lat_rad.cos() * (math.pi * R) / 180
    return km_per_lat, km_per_lon
