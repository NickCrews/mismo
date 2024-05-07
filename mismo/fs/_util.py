from __future__ import annotations

import math
from typing import Union, overload

import ibis
from ibis import Deferred
from ibis.expr import types as ir

_IbisIsh = Union[Deferred, ir.NumericValue]


@overload
def prob_to_odds(prob: float) -> float: ...


@overload
def prob_to_odds(prob: _IbisIsh) -> _IbisIsh: ...


def prob_to_odds(prob):
    normal = prob / (1 - prob)
    if isinstance(prob, (Deferred, ir.NumericValue)):
        return ibis.ifelse(prob != 1, normal, ibis.literal(float("inf")))
    else:
        return normal if prob != 1 else float("inf")


@overload
def odds_to_prob(odds: float) -> float: ...


@overload
def odds_to_prob(odds: _IbisIsh) -> _IbisIsh: ...


def odds_to_prob(odds):
    normal = odds / (1 + odds)
    if isinstance(odds, (Deferred, ir.NumericValue)):
        return ibis.ifelse(odds.isinf(), 1.0, normal)
    else:
        return 1 if odds == float("inf") else normal


@overload
def odds_to_log_odds(odds: float) -> float: ...


@overload
def odds_to_log_odds(odds: _IbisIsh) -> _IbisIsh: ...


def odds_to_log_odds(odds):
    if isinstance(odds, (Deferred, ir.NumericValue)):
        return odds.log10()
    else:
        if odds == 0:
            return float("-inf")
        else:
            return math.log10(odds)
