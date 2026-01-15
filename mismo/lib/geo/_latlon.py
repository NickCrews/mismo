from __future__ import annotations

from collections.abc import Mapping
import dataclasses
import math
from typing import Callable, Literal, TypedDict

import ibis
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


Coordinates = Mapping[Literal["lat", "lon"], ir.FloatingColumn]
CoordinatesMapping = Mapping[Literal["lat", "lon"], _util.IntoValue]


class CoordinatesDict(TypedDict):
    lat: ir.FloatingColumn
    lon: ir.FloatingColumn


IntoCoordinates = (
    str
    | ibis.Deferred
    | Coordinates
    | CoordinatesMapping
    | Callable[[ir.Table], "IntoCoordinates"]
)


def default_resolver() -> CoordinatesMapping:
    return {"lat": "lat", "lon": "lon"}


def get_coordinate_pair(
    table: ir.Table,
    mapping: IntoCoordinates,
) -> CoordinatesDict:
    """Get the coordinates from a table.

    Parameters
    ----------
    table
        The table to get the coordinates from.
    mapping
        A mapping of column names to use for the coordinates,
        or a function that takes a table and returns the coordinate pair.

    Returns
    -------
    coordinates
        The coordinates.
    """
    if callable(mapping):
        called = mapping(table)
        return get_coordinate_pair(table, called)

    if isinstance(mapping, str) or isinstance(mapping, ibis.Deferred):
        coords_col = _util.bind_one(table, mapping)
        lat: ir.FloatingColumn = coords_col["lat"]  # ty:ignore[not-subscriptable]
        lon: ir.FloatingColumn = coords_col["lon"]  # ty:ignore[not-subscriptable]
    else:
        lat: ir.FloatingColumn = _util.bind_one(table, mapping["lat"])  # ty:ignore[invalid-assignment]
        lon: ir.FloatingColumn = _util.bind_one(table, mapping["lon"])  # ty:ignore[invalid-assignment]
    return CoordinatesDict(lat=lat, lon=lon)


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
    ...     left_resolver="latlon",
    ...     right_resolver={"lat": "latitude", "lon": "longitude"},
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
    max_pairs: int | None = None
    """The maximum number of pairs that any single block of coordinates can contain.
    
    eg if you have 1000 records all with the same coordinates, this would
    naively result in ~(1000 * 1000) / 2 = 500_000 pairs.
    If we set max_pairs to less than this, this group of records will be skipped.
    """
    left_resolver: IntoCoordinates = dataclasses.field(default_factory=default_resolver)
    """A specification of how to get the lat and lon values from the left table.
    
    Can be:
    - A `str` or `ibis.Deferred`, which are assumed to point to a column
      containing a struct with `lat` and `lon` fields.
    - A `Mapping` of `{"lat": ..., "lon": ...}` where the values are
      column names or `ibis.Deferred` expressions.
    - A callable that takes a table and returns one of the above.
    """
    right_resolver: IntoCoordinates = dataclasses.field(
        default_factory=default_resolver
    )
    """See `left_resolver`, but for the right table."""

    def hash_coord(
        self, coord: CoordinatesDict, /
    ) -> tuple[ir.IntegerValue, ir.IntegerValue]:
        lat, lon = coord["lat"], coord["lon"]
        # We have to use a grid size of ~3x the precision to avoid
        # two points falling right on either side of a grid cell boundary
        grid_size = self.distance_km * 3
        lat_key, lon_key = _bin_lat_lon(lat, lon, grid_size)
        return lat_key, lon_key

    def hash_left(self, t: ir.Table, /) -> tuple[ir.IntegerValue, ir.IntegerValue]:
        coords = get_coordinate_pair(t, self.left_resolver)
        return self.hash_coord(coords)

    def hash_right(self, t: ir.Table, /) -> tuple[ir.IntegerValue, ir.IntegerValue]:
        coords = get_coordinate_pair(t, self.right_resolver)
        return self.hash_coord(coords)

    @property
    def _key_linker(self) -> KeyLinker:
        import mismo

        def lat_key_left(t: ir.Table) -> ir.IntegerValue:
            return self.hash_left(t)[0]

        def lon_key_left(t: ir.Table) -> ir.IntegerValue:
            return self.hash_left(t)[1]

        def lat_key_right(t: ir.Table) -> ir.IntegerValue:
            return self.hash_right(t)[0]

        def lon_key_right(t: ir.Table) -> ir.IntegerValue:
            return self.hash_right(t)[1]

        return mismo.KeyLinker(
            [(lat_key_left, lat_key_right), (lon_key_left, lon_key_right)],
            max_pairs=self.max_pairs,
        )

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
