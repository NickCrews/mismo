from __future__ import annotations

import ibis

from mismo import _util


class MismoError(Exception):
    """Base class for all Mismo errors."""


class MismoWarning(Warning):
    """Base class for all Mismo warnings."""


class UnsupportedBackendError(ValueError, MismoError):
    """An operation is not supported by a particular backend."""


class _SlowJoinMixin:
    def __init__(self, condition: ibis.ir.BooleanValue, algorithm: str) -> None:
        self.condition = condition
        self.algorithm = algorithm
        super().__init__(
            f"The join '{_util.get_name(self.condition)}' uses the {algorithm} algorithm and is likely to be slow."  # noqa: E501
        )


class SlowJoinWarning(_SlowJoinMixin, UserWarning, MismoWarning):
    """Warning for slow join algorithms."""


class SlowJoinError(_SlowJoinMixin, ValueError, MismoError):
    """Error for slow join algorithms."""
