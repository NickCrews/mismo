from __future__ import annotations

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
        max_frequency: float = 1 / 1_000_000,
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
        keys_right = [
            right.name(left.get_name()) for left, right in zip(keys_left, keys_right)
        ]
        model = _tf.TermFrequencyModel(keys_left)
        freq_name = _util.unique_name("freq")
        with_frequencies = model.term_frequencies(needle, name_as=freq_name)
        filtered = with_frequencies.filter(_[freq_name] <= self.max_frequency).drop(
            freq_name
        )
        return filtered

    def __join_condition__(
        self,
        haystack: ibis.Table,
        needle: ibis.Table,
    ) -> ir.BooleanColumn:
        filtered = self.rare_needle(haystack=haystack, needle=needle)
        return filtered.record_id.isin(filtered.record_id)
