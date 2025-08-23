from __future__ import annotations

import dataclasses
import math
from typing import Callable

import ibis
from ibis import Deferred
from ibis.expr import types as ir

from mismo import _util, linkage, types
from mismo._counts_table import KeyCountsTable, PairCountsTable
from mismo.linker._key_linker import KeyLinker


def distance_km(
    *,
    lat1: ir.FloatingValue,
    lon1: ir.FloatingValue,
    lat2: ir.FloatingValue,
    lon2: ir.FloatingValue,
) -> ir.FloatingValue:
    """The distance between two points on the Earth's surface, in kilometers.

    Parameters
    ----------
    lat1:
        The latitude of the first point.
    lon1:
        The longitude of the first point.
    lat2:
        The latitude of the second point.
    lon2:
        The longitude of the second point.

    Returns
    -------
    distance:
        The distance between the two points, in kilometers.
    """

    def radians(degrees):
        scaling = math.pi / 180
        return degrees * scaling

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    def haversine(theta: ir.FloatingValue) -> ir.FloatingValue:
        return (theta / 2).sin() ** 2

    a = haversine(lat2 - lat1) + lat1.cos() * lat2.cos() * haversine(lon2 - lon1)
    R_earth = 6371.0  # km
    # parens for optimization, to make sure the division happens on the python side
    return (R_earth * 2) * a.sqrt().asin()


@dataclasses.dataclass(frozen=True)
class CoordinateLinker:
    """Links two locations together if they are within a certain distance.

    This isn't precise, and can include pairs that are actually up to about 2x
    larger than the given threshold.
    This is because we use a simple grid to bin the coordinates, so
    1. This isn't accurate near the poles, and
    2. This isn't accurate near the international date line (longitude 180/-180).
    3. If two coords fall within opposite corners of the same grid cell, they
        will be blocked together even if they are further apart than the
        precision, due to the diagonal distance being longer than the horizontal
        or vertical distance.

    Examples
    --------
    >>> import ibis
    >>> from mismo.lib.geo import CoordinateLinker
    >>> ibis.options.interactive = True
    >>> conn = ibis.duckdb.connect()
    >>> left = conn.create_table(
    ...     "left",
    ...     [
    ...         {
    ...             "record_id": 0,
    ...             "latlon": {"lat": 61.1547800, "lon": -150.0677490},
    ...         }
    ...     ],
    ... )
    >>> right = conn.create_table(
    ...     "right",
    ...     [
    ...         {
    ...             "record_id": 4,
    ...             "latitude": 61.1582056,
    ...             "longitude": -150.0584552,
    ...         },
    ...         {
    ...             "record_id": 5,
    ...             "latitude": 61.1582056,
    ...             "longitude": 0,
    ...         },
    ...         {
    ...             "record_id": 6,
    ...             "latitude": 61.1547800,
    ...             "longitude": -150,
    ...         },
    ...     ],
    ... )
    >>> linker = CoordinateLinker(
    ...     distance_km=1,
    ...     left_coord="latlon",
    ...     right_lat="latitude",
    ...     right_lon="longitude",
    ... )
    >>> linker(left, right).links
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ latlon_l                      ┃ record_id_r ┃ latitude_r ┃ longitude_r ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
    │ int64       │ struct<lat: float64, lon:     │ int64       │ float64    │ float64     │
    │             │ float64>                      │             │            │             │
    ├─────────────┼───────────────────────────────┼─────────────┼────────────┼─────────────┤
    │             │ {                             │             │            │             │
    │           0 │     'lat': 61.15478,          │           4 │  61.158206 │ -150.058455 │
    │             │     'lon': -150.067749        │             │            │             │
    │             │ }                             │             │            │             │
    └─────────────┴───────────────────────────────┴─────────────┴────────────┴─────────────┘
    """  # noqa: E501

    distance_km: float | int
    """
    The (approx) max distance in kilometers that two coords will be blocked together.
    """
    coord: str | Deferred | Callable[[ir.Table], ir.StructColumn] | None = None
    """The column in both tables containing the `struct<lat: float, lon: float>` coordinates."""  # noqa: E501
    lat: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in both tables containing the latitude coordinates."""
    lon: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in both tables containing the longitude coordinates."""
    left_coord: str | Deferred | Callable[[ir.Table], ir.StructColumn] | None = None
    """The column in the left tables containing the `struct<lat: float, lon: float>` coordinates."""  # noqa: E501
    right_coord: str | Deferred | Callable[[ir.Table], ir.StructColumn] | None = None
    """The column in the right tables containing the `struct<lat: float, lon: float>` coordinates."""  # noqa: E501
    left_lat: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in the left tables containing the latitude coordinates."""
    left_lon: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in the left tables containing the longitude coordinates."""
    right_lat: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in the right tables containing the latitude coordinates."""
    right_lon: str | Deferred | Callable[[ir.Table], ir.FloatingColumn] | None = None
    """The column in the right tables containing the longitude coordinates."""
    max_pairs: int | None = None
    """The maximum number of pairs that any single block of coordinates can contain.
    
    eg if you have 1000 records all with the same coordinates, this would
    naively result in ~(1000 * 1000) / 2 = 500_000 pairs.
    If we set max_pairs to less than this, this group of records will be skipped.
    """

    def __post_init__(self):
        ok_subsets = [
            {"coord"},
            {"left_coord", "right_coord"},
            {"left_coord", "right_lat", "right_lon"},
            {"left_lat", "left_lon", "right_coord"},
            {"left_lat", "left_lon", "right_lat", "right_lon"},
            {"lat", "lon"},
            {"lat", "right_lat", "right_lon"},
            {"left_lat", "left_lon", "lon"},
        ]
        options = [
            "coord",
            "left_coord",
            "right_coord",
            "lat",
            "lon",
            "left_lat",
            "left_lon",
            "right_lat",
            "right_lon",
        ]
        present = {k for k in options if getattr(self, k) is not None}
        if present not in ok_subsets:
            ok_subsets_str = "\n".join("- " + str(s) for s in ok_subsets)
            raise ValueError(
                "You must specify exactly one of the following subsets of options:\n"
                + ok_subsets_str
                + f"\nYou provided:\n{present}"
            )

    def _left_coord(
        self, left: ir.Table
    ) -> tuple[ir.FloatingColumn, ir.FloatingColumn]:
        if self.coord is not None:
            left_coord = _util.get_column(left, self.coord, on_many="struct")
            return (left_coord.lat, left_coord.lon)
        if self.left_coord is not None:
            left_coord = _util.get_column(left, self.left_coord, on_many="struct")
            left_lat = left_coord.lat
            left_lon = left_coord.lon
        if self.lat is not None:
            left_lat = _util.get_column(left, self.lat)
        if self.lon is not None:
            left_lon = _util.get_column(left, self.lon)
        if self.left_lat is not None:
            left_lat = _util.get_column(left, self.left_lat)
        if self.left_lon is not None:
            left_lon = _util.get_column(left, self.left_lon)
        return left_lat, left_lon

    def _right_coord(
        self, right: ir.Table
    ) -> tuple[ir.FloatingColumn, ir.FloatingColumn]:
        if self.coord is not None:
            right_coord = _util.get_column(right, self.coord, on_many="struct")
            return (right_coord.lat, right_coord.lon)
        if self.right_coord is not None:
            right_coord = _util.get_column(right, self.right_coord, on_many="struct")
            right_lat = right_coord.lat
            right_lon = right_coord.lon
        if self.lat is not None:
            right_lat = _util.get_column(right, self.lat)
        if self.lon is not None:
            right_lon = _util.get_column(right, self.lon)
        if self.right_lat is not None:
            right_lat = _util.get_column(right, self.right_lat)
        if self.right_lon is not None:
            right_lon = _util.get_column(right, self.right_lon)
        return right_lat, right_lon

    def _join_keys(
        self, left: ir.Table | ibis.Deferred, right: ir.Table | ibis.Deferred
    ) -> tuple[tuple[ir.Column, ir.Column], tuple[ir.Column, ir.Column]]:
        left_lat, left_lon = self._left_coord(left)
        right_lat, right_lon = self._right_coord(right)
        # We have to use a grid size of ~3x the precision to avoid
        # two points falling right on either side of a grid cell boundary
        grid_size = self.distance_km * 3
        left_lat_key, left_lon_key = _bin_lat_lon(left_lat, left_lon, grid_size)
        right_lat_key, right_lon_key = _bin_lat_lon(right_lat, right_lon, grid_size)
        return (
            left_lat_key.name("lat_binned"),
            right_lat_key.name("lat_binned"),
        ), (
            left_lon_key.name("lon_binned"),
            right_lon_key.name("lon_binned"),
        )

    @property
    def _key_linker(self) -> KeyLinker:
        import mismo

        lat_key, lon_key = self._join_keys(ibis._, ibis._)
        return mismo.KeyLinker([lat_key, lon_key], max_pairs=self.max_pairs)

    def __join_condition__(self, left: ir.Table, right: ir.Table) -> ir.BooleanValue:
        return self._key_linker.__join_condition__(left, right)

    def __call__(self, left: ir.Table, right: ir.Table) -> linkage.Linkage:
        links = types.LinksTable.from_join_condition(left, right, self)
        return linkage.Linkage(left=left, right=right, links=links)

    def key_counts_left(self, left: ibis.Table, /) -> KeyCountsTable:
        return self._key_linker.key_counts_left(left)

    def key_counts_right(self, right: ibis.Table, /) -> KeyCountsTable:
        return self._key_linker.key_counts_right(right)

    def pair_counts(self, left: ibis.Table, right: ibis.Table) -> PairCountsTable:
        return self._key_linker.pair_counts(left, right)


def _bin_lat_lon(
    lat: ir.FloatingValue, lon: ir.FloatingValue, grid_size_km: float | int
) -> tuple[ir.IntegerValue, ir.IntegerValue]:
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
    if ibis.__version__ >= "10.0.0":
        # See https://github.com/ibis-project/ibis/pull/10353
        lat_hash = lat // step_size_lat
        lon_hash = lon // step_size_lon
    else:
        lat_hash = (lat / step_size_lat).floor().cast(int)
        lon_hash = (lon / step_size_lon).floor().cast(int)
    both_null = lat.isnull() & lon.isnull()
    lat_result = both_null.ifelse(ibis.null(), lat_hash)
    lon_result = both_null.ifelse(ibis.null(), lon_hash)
    return lat_result, lon_result


def _km_per_degree(lat: ir.FloatingValue) -> tuple[float, ir.FloatingValue]:
    # Radius of the Earth in kilometers
    R = 6371.0
    lat_rad = lat * (math.pi / 180)
    # This is a constant value at any given latitude.
    km_per_lat = (math.pi * R) / 180
    # This varies based on the latitude.
    # Parens for optimization, to make sure the division happens on the python side.
    km_per_lon = lat_rad.cos() * ((math.pi * R) / 180)
    return km_per_lat, km_per_lon
