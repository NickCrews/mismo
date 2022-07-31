import logging
from typing import Any, Callable

import pyarrow as pa
import spacy
from vaex.dataframe import DataFrame
from vaex.expression import Expression

from mismo.block._fingerprint import SingleColumnFingerprinter, add_index

logger = logging.getLogger(__name__)

_nlp = None


def _load_spacy():
    global _nlp
    if _nlp is None:
        _nlp = spacy.load("en_core_web_sm", exclude=["ner", "entity_linker", "textcat"])
    return _nlp


def norm_whitespace(texts: Expression) -> Expression:
    return texts.str.strip().str.replace(r"\s+", " ", regex=True).astype(pa.string())


def norm_possessives(texts: Expression) -> Expression:
    """Fix "jane's house" to "janes house".

    TODO: Also deal with "Ross' house"
    """
    return texts.str.replace(r"(\w)\'s(\b)", r"\1s\2", regex=True)


_token_fields = [
    ("text", pa.string()),
    ("lemma", pa.string()),
    ("pos", pa.string()),
]
_token_type = pa.struct(_token_fields)
_doc_type = pa.list_(_token_type)


def _tokenize_chunk(s: pa.StringArray) -> pa.ListArray:
    """Apply spacy tokenier to a pyarrow StringArray."""
    logger.debug(f"tokenizing {len(s)} strings...")
    strs = (str(s) for s in s)
    nlp = _load_spacy()
    docs = nlp.pipe(strs)
    token_lists = ([(str(tok), tok.lemma_, tok.pos_) for tok in doc] for doc in docs)
    result = pa.array(token_lists, type=_doc_type)
    masked = pa.compute.if_else(s.is_null(), None, result)
    return masked


def tokenize(texts: Expression) -> Expression:
    """Returns an expression where each item is a list of `token` structs.

    Each token has the fields "text", "lemma" and "pos", corresponding to a spacy.Token
    """
    texts = norm_whitespace(texts)
    # vaex will apply this in chunk sizes per config:
    # https://vaex.readthedocs.io/en/latest/conf.html#chunk
    # spacy already does multiprocessing so don't do that here
    return texts.apply(_tokenize_chunk, vectorize=True, multiprocessing=False)


def filter_tokens(
    tokens: DataFrame, predicate: Callable[[Expression], Any]
) -> DataFrame:
    keep = predicate(tokens["token"])
    return tokens[keep]


class StringBaseFingerprinter(SingleColumnFingerprinter):
    def __init__(
        self,
        column: str,
        lower: bool = True,
        norm_possesive: bool = True,
        norm_whitespace: bool = True,
    ) -> None:
        super().__init__(column=column)
        self.lower = lower
        self.norm_possesive = norm_possesive
        self.norm_whitespace = norm_whitespace

    def _preprocess(self, data: DataFrame) -> Expression:
        strings = data[self.column]
        if self.lower:
            strings = strings.str.lower()
        if self.norm_possesive:
            strings = norm_possessives(strings)
        if self.norm_whitespace:
            strings = norm_whitespace(strings)
        result = data.copy()
        result[self.column] = strings
        return result

    def fingerprint(self, data: DataFrame) -> DataFrame:
        """Selects the column of data, preprocesses it, and passes to _func."""
        data = self._preprocess(data)
        result = self._func(data)
        return result


class TokenFingerprinter(StringBaseFingerprinter):
    def _func(self, df: DataFrame) -> DataFrame:
        result = df.copy([self.column])
        result["tokens"] = tokenize(result[self.column])
        result = add_index(result)
        result = result[result["tokens"].notna()]
        result = result.mismo.explode("tokens")
        result["token"] = result["tokens"].struct_get("lemma")
        result = result.drop([self.column, "tokens"])
        return result


class SortedAcronymFingerprinter(StringBaseFingerprinter):
    def __init__(self, column: str, only_alpha: bool = True) -> None:
        super().__init__(column, lower=True, norm_possesive=True)
        self.only_alpha = only_alpha

    def _func(self, strings: Expression) -> DataFrame:
        tokens = tokenize(strings)
        tokens = filter_tokens(tokens, lambda token: token.str.len() > 1)
        tokens["first_char"] = tokens["token"].str.slice(0, 1)
        tokens = tokens.drop(columns="token")
        if self.only_alpha:
            tokens = tokens[tokens["first_char"].str.isalpha()]
        tokens = tokens.sort_values(by=["index", "first_char"])
        acronyms = tokens.groupby("index", as_index=False).agg("sum")
        return acronyms
