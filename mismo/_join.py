from __future__ import annotations

import ibis
from ibis.expr.types import Table


def join(
    left: Table,
    right: Table,
    predicates=tuple(),
    how="inner",
    *,
    lname: str = "",
    rname: str = "{name}_right",
) -> Table:
    """Similar to ibis's join, with a few differences

    - Does a cross join when predicates is True or how is "cross"
    - Converts the lname and rname suffixes to the appropriate kwargs for ibis<6.0.0
    - Allows for a wider set of join predicates:
      - anything that ibis accepts as a join predicate
      - tuple[str, str]
      - tuple[Column, Column]
      - tuple[Deferred, Deferred]
      - lambda (left, right) -> any of the above
    """
    rename_kwargs = _join_suffix_kwargs(lname=lname, rname=rname)
    preds = _to_ibis_join_predicates(left, right, predicates)
    return left.join(right, predicates=preds, how=how, **rename_kwargs)


def _to_ibis_join_predicates(left, right, raw_predicates) -> tuple:
    if isinstance(raw_predicates, tuple):
        if len(raw_predicates) != 2:
            raise ValueError(
                f"predicates must be a tuple of length 2, got {raw_predicates}"
            )
        # Ibis has us covered with one adjustment
        # https://github.com/ibis-project/ibis/pull/7424
        return [raw_predicates]
    if callable(raw_predicates):
        return _to_ibis_join_predicates(left, right, raw_predicates(left, right))
    else:
        return raw_predicates


def _join_suffix_kwargs(lname: str, rname: str) -> dict:
    """create the suffix kwargs for ibis.join(), no matter the ibis version.

    The suffixes kwarg got split into lname and rname in ibis 6.0.0:
    https://ibis-project.org/release_notes/#600-2023-07-05"""
    if ibis.__version__ >= "6.0.0":
        return {"lname": lname, "rname": rname}
    else:

        def _convert_suffix(suffix: str) -> str:
            if not len(suffix):
                return ""
            if not suffix.startswith("{name}"):
                raise ValueError(
                    "suffix must be empty or start with '{name}'"
                    f"for ibis<6.0.0, got {suffix}"
                )
            return suffix.removeprefix("{name}")

        lsuffix = _convert_suffix(lname)
        rsuffix = _convert_suffix(rname)
        return {"suffixes": (lsuffix, rsuffix)}
