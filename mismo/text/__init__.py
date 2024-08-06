"""Functionalities for text processing and analysis."""

from __future__ import annotations

from mismo.text._features import ngrams as ngrams
from mismo.text._similarity import damerau_levenshtein as damerau_levenshtein
from mismo.text._similarity import (
    damerau_levenshtein_ratio as damerau_levenshtein_ratio,
)
from mismo.text._similarity import double_metaphone as double_metaphone
from mismo.text._similarity import levenshtein_ratio as levenshtein_ratio
from mismo.text._strings import norm_whitespace as norm_whitespace
