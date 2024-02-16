from __future__ import annotations

import math

from ibis.expr.types import FloatingValue


def distance_km(
    *,
    lat1: FloatingValue,
    lon1: FloatingValue,
    lat2: FloatingValue,
    lon2: FloatingValue,
) -> FloatingValue:
    """The distance between two points on the Earth's surface, in kilometers.

    Parameters
    ----------
    lat1 : FloatingValue
        The latitude of the first point.
    lon1 : FloatingValue
        The longitude of the first point.
    lat2 : FloatingValue
        The latitude of the second point.
    lon2 : FloatingValue
        The longitude of the second point.

    Returns
    -------
    distance : FloatingValue
        The distance between the two points, in kilometers.
    """

    def radians(degrees):
        scaling = math.pi / 180
        return degrees * scaling

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    def haversine(theta: FloatingValue) -> FloatingValue:
        return (theta / 2).sin() ** 2

    a = haversine(lat2 - lat1) + lat1.cos() * lat2.cos() * haversine(lon2 - lon1)
    c = 2 * a.sqrt().asin()
    R_earth = 6371.0  # km
    return R_earth * c
