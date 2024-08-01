"""Functionalities for text processing and analysis."""

from __future__ import annotations

from mismo.text._similarity import damerau_levenshtein as damerau_levenshtein
from mismo.text._similarity import double_metaphone as double_metaphone
from mismo.text._similarity import levenshtein_ratio as levenshtein_ratio
from mismo.text._similarity import token_set_ratio as token_set_ratio
from mismo.text._similarity import token_sort_ratio as token_sort_ratio
from mismo.text._similarity import partial_token_sort_ratio as partial_token_sort_ratio
from mismo.text._strings import ngrams as ngrams
from mismo.text._strings import norm_whitespace as norm_whitespace
