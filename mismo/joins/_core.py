from __future__ import annotations

from typing import TYPE_CHECKING

import ibis
from ibis.expr import types as ir

from mismo.joins import _conditions

if TYPE_CHECKING:
    from collections.abc import Iterable


def join(
    left: ibis.Table,
    right: ibis.Table,
    predicates: _conditions.IntoHasJoinCondition = (),
    how: str = "inner",
    *,
    lname: str = "{name}",
    rname: str = "{name}_right",
    rename_all: bool = False,
):
    """
    Ibis.join, but with enhanced condition resolution.

    This is a wrapper around `ibis.join` that

    - Allows for slightly more flexible join conditions.
    - Adds an option for renaming *all* columns with `lname` and `rname`,
      not just those that collide (the default behavior).

    Parameters
    ----------
    left
        The left table to block
    right
        The right table to block
    predicates
        What `ibis.join()` accepts as a predicate, plus some extras.

        Anything that `ibis.join()` accepts as a predicate:
        - An `ibis.BooleanValue`, such as `left.last_name == right.surname.upper()`.
        - A string, which is interpreted as the name of a column in both tables.
          eg "price" is equivalent to `left.price == right.price`.
        - A Deferred, which is used to reference a column in a table
          eg "_.price.fill_null(0)" is equivalent to `left.price.fill_null(0) == right.price.fill_null(0)`

        Plus some extra sorts of inputs:
        - A 2-tuple tuple of one of the above.
          The first element is for the left table, the second is for the right table.
          This is useful when the column names are different in the two tables,
          or require some transformation/normalization.
          For example `("last_name", _.surname.upper()")` is equivalent to
          `left.last_name == right.surname.upper()`
        - A Callable of signature (left: Table, right: Table, args) -> one of the above
    how
        Behaves the the same as in `ibis.join()`
    lname
        Behaves the the same as in `ibis.join()`
    rname
        Behaves the the same as in `ibis.join()`
    rename_all
        Should we apply `lname` and `rname` to ALL columns in the output,
        or just on the ones that collide between the two tables
        (the default, the same as in `ibis.join()`)
    """  # noqa: E501
    condition = _conditions.join_condition(predicates)
    resolved_predicate = condition.__join_condition__(left, right)
    joined = ibis.join(
        left,
        right,
        how=how,
        lname=lname,
        rname=rname,
        predicates=resolved_predicate,
    )
    if rename_all:
        joined = rename_all_joins(
            left=left, right=right, joined=joined, lname=lname, rname=rname
        )
    return joined


def rename_all_joins(
    *,
    left: ibis.Table,
    right: ibis.Table,
    joined: ibis.Table,
    lname: str = "{name}",
    rname: str = "{name}_right",
):
    """
    Adjust result of Ibis.join, always applying lname and rname, not just on conflict.
    """

    def _rename(spec: str, name: str):
        return spec.format(name=name)

    selections = []
    for col in left.columns:
        new_name = _rename(lname, col)
        if new_name in joined.columns:
            selections.append((new_name, new_name))
        else:
            assert col in joined.columns
            selections.append((new_name, col))
    for col in right.columns:
        new_name = _rename(rname, col)
        if new_name in joined.columns:
            selections.append((new_name, new_name))
        else:
            assert col in joined.columns
            selections.append((new_name, col))

    # check for dupe output columns
    by_new_name = {}
    for new_name, old_name in selections:
        if new_name not in by_new_name:
            by_new_name[new_name] = [old_name]
        else:
            by_new_name[new_name].append(old_name)
    dupes = []
    for new_name, old_names in by_new_name.items():
        if len(old_names) > 1:
            dupes.append(f"Column {new_name} is produced by {old_names}")
    if dupes:
        raise ValueError("\n".join(dupes))

    return joined.select(**{new_name: old_name for new_name, old_name in selections})


def remove_condition_overlap(
    conditions: Iterable[ir.BooleanValue | bool],
) -> list[ir.BooleanValue]:
    """
    Constrain each condition to not generate any pairs that are already created by previous conditions.
    """  # noqa: E501
    result = []
    priors = []
    for condition in conditions:
        if isinstance(condition, bool):
            condition = ibis.literal(condition)
        modified = ibis.and_(condition, *[~prior for prior in priors])
        priors.append(condition)
        result.append(modified)
    return result
