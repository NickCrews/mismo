from __future__ import annotations

from typing import overload

import ibis
import math
from ibis.expr.types import FloatingValue


@overload
def prob_to_odds(prob: float) -> float:
    ...


@overload
def prob_to_odds(prob: FloatingValue) -> FloatingValue:
    ...


def prob_to_odds(prob):
    normal = prob / (1 - prob)
    if isinstance(prob, FloatingValue):
        return ibis.ifelse(prob != 1, normal, ibis.literal(float("inf")))
    else:
        return normal if prob != 1 else float("inf")


@overload
def odds_to_prob(odds: float) -> float:
    ...


@overload
def odds_to_prob(odds: FloatingValue) -> FloatingValue:
    ...


def odds_to_prob(odds):
    normal = odds / (1 + odds)
    if isinstance(odds, FloatingValue):
        return ibis.ifelse(odds.isinf(), 1.0, normal)
    else:
        return 1 if odds == float("inf") else normal


@overload
def odds_to_log_odds(odds: float) -> float:
    ...


@overload
def odds_to_log_odds(odds: FloatingValue) -> FloatingValue:
    ...


def odds_to_log_odds(odds):
    if isinstance(odds, FloatingValue):
        return odds.log10()
    else:
        return math.log10(odds)
