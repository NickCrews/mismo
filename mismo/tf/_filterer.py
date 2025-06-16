from __future__ import annotations

from fractions import Fraction
from typing import Any

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo import _resolve, _util
from mismo.tf import _tf


class RareLookupFilterer:
    """When looking up a needle in a haystack, remove needles that are too common.

    Consider the name "Nick Morey".
    The names "Nick" and "Morey" are individually moderately common,
    say 1/1_000 and 1/1_000. If the columns are independent,
    then if you see a "Nick Morey" in the needle, there is a 1 / 1_000_000
    chance that this is actuallyy referring to a different person.

    This is actually a bad assumption for names, eg "mohammed ali"
    is actually probably pretty common, even though mohammed and ali
    are rare when considered individually.
    But, I'd rather avoid false links than real ones.
    """

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

    def filter_common(
        self, haystack: ibis.Table, needle: ibis.Table
    ) -> ir.BooleanColumn:
        rare_needle = self.rare_needle(haystack=haystack, needle=needle)
        return needle.record_id.isin(rare_needle.record_id)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"column_resolvers={self.column_resolvers}, "
            f"max_frequency={self.max_frequency})"
        )


class AmbiguousHaystackFilterer:
    """Remove links where multiple records in the haystack would be matched.

    When looking up "John Smith" in the haystack, since that is a common name,
    it will appear multiple times. Thus, if there is a John Smith in the needle,
    it would be ambiguous which of these haystack records we should link to.
    So, we should throw out any links where the haystack value appears multiple time.
    """

    def __init__(self, column_resolvers: Any) -> None:
        self.column_resolvers = _resolve.key_pair_resolvers(column_resolvers)

    def ambiguous_haystack(
        self,
        *,
        haystack: ibis.Table,
        needle: ibis.Table,
    ) -> ibis.ir.IntegerColumn:
        # I'm looking for not member of dupe because I assume the set of dupes
        # is going to be smaller than the number of nondupes, and I
        # THINK that would lead to a faster isin check?
        key_pairs = [resolver(haystack, needle) for resolver in self.column_resolvers]
        keys_left, keys_right = zip(*key_pairs)
        n = haystack.count().over(group_by=keys_left)
        return haystack.filter(n > 1)

    def filter_common(
        self, haystack: ibis.Table, needle: ibis.Table
    ) -> ir.BooleanColumn:
        ambiguous_haystack = self.ambiguous_haystack(haystack=haystack, needle=needle)
        return haystack.record_id.notin(ambiguous_haystack.record_id)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(column_resolvers={self.column_resolvers}, "
