from __future__ import annotations

from typing import overload

import ibis
from ibis.expr.types import FloatingValue


@overload
def prob_to_bayes_factor(prob: float) -> float:
    ...


@overload
def prob_to_bayes_factor(prob: FloatingValue) -> FloatingValue:
    ...


def prob_to_bayes_factor(prob):
    normal = prob / (1 - prob)
    if isinstance(prob, FloatingValue):
        return ibis.ifelse(prob != 1, normal, ibis.literal(float("inf")))
    else:
        return normal if prob != 1 else float("inf")
