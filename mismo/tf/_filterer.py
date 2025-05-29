from __future__ import annotations

from fractions import Fraction
from typing import Any

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _resolve, _util
from mismo.tf import _tf


class RareLookupFilterer:
    """When looking up a needle in a haystack, remove needles that are too common."""

    def __init__(
        self,
        column_resolvers: Any,
        *,
        max_frequency: float | Fraction,
    ) -> None:
        self.column_resolvers = _resolve.key_pair_resolvers(column_resolvers)
        self.max_frequency = max_frequency

    def rare_needle(
        self,
        *,
        haystack: ibis.Table,
        needle: ibis.Table,
    ) -> ibis.Table:
        key_pairs = [resolver(haystack, needle) for resolver in self.column_resolvers]
        keys_left, keys_right = zip(*key_pairs)
        needle_columns = {left.get_name(): right for left, right in key_pairs}
        model = _tf.TermFrequencyModel(keys_left)
        freq_name = _util.unique_name("freq")
        with_frequencies = model.add_term_frequencies(
            needle, columns=needle_columns, name_as=freq_name
        )
        filtered = with_frequencies.filter(
            _[freq_name] <= float(self.max_frequency)
        ).drop(freq_name)
        return filtered

    def __join_condition__(
        self,
        haystack: ibis.Table,
        needle: ibis.Table,
    ) -> ir.BooleanColumn:
        rare_needle = self.rare_needle(haystack=haystack, needle=needle)
        return needle.record_id.isin(rare_needle.record_id)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"column_resolvers={self.column_resolvers}, "
            f"max_frequency={self.max_frequency})"
        )
