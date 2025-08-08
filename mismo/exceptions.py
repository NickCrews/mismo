from __future__ import annotations

import typing

import ibis

from mismo import _util

if typing.TYPE_CHECKING:
    from mismo.joins._analyze import SlowJoinAlgorithm


class MismoError(Exception):
    """Base class for all Mismo errors."""


class MismoWarning(Warning):
    """Base class for all Mismo warnings."""


class UnsupportedBackendError(ValueError, MismoError):
    """An operation is not supported by a particular backend."""


class SlowJoinMixin:
    def __init__(
        self, condition: ibis.ir.BooleanValue, algorithm: SlowJoinAlgorithm
    ) -> None:
        self.condition: ibis.ir.BooleanValue = condition
        """The join condition that is slow."""
        self.algorithm: SlowJoinAlgorithm = algorithm
        """The algorithm used for the join, eg 'NESTED_LOOP_JOIN'."""
        super().__init__(
            f"The join '{_util.get_name(self.condition)}' uses the {algorithm} algorithm and is likely to be slow."  # noqa: E501
        )


class SlowJoinWarning(SlowJoinMixin, UserWarning, MismoWarning):
    """Warning for slow join algorithms."""


class SlowJoinError(SlowJoinMixin, ValueError, MismoError):
    """Error for slow join algorithms."""
