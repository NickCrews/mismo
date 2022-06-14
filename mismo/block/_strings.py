from typing import Any, Callable, Iterable

import pandas as pd
import spacy
from spacy.tokens import Doc

from mismo._typing import Data
from mismo.block._fingerprint import SingleColumnFingerprinter, add_index

nlp = spacy.load("en_core_web_sm", exclude=["ner", "entity_linker", "textcat"])


def norm_whitespace(texts: pd.Series) -> pd.Series:
    return texts.str.strip().str.replace(r"\s+", " ", regex=True)


def norm_possessives(texts: pd.Series) -> pd.Series:
    """Fix "jane's house" to "janes house".

    TODO: Also deal with "Ross' house"
    """
    return texts.str.replace(r"(\w)\'s(\b)", r"\1s\2", regex=True)


def tokenize(texts: pd.Series) -> pd.DataFrame:
    """Returns a DF with two columns, "index" and "token"

    One token per row, so if entry 2 in input has three tokens,
    then there will be three rows for this entry, all with index 2.

    Each token is a spacy.Token object.
    """
    texts = norm_whitespace(texts)
    notna = texts.notna() & (texts.str.len() > 0)
    index = pd.RangeIndex(0, len(texts))[notna]
    docs: Iterable[Doc] = nlp.pipe(texts[notna])
    token_lists = pd.Series((list(doc) for doc in docs), name="tokens")
    tokens_per_input = add_index(token_lists, index=index)
    result = tokens_per_input.explode("tokens", ignore_index=True)
    result = result.rename(columns={"tokens": "token"})
    return result


def filter_tokens(
    tokens: pd.DataFrame, predicate: Callable[[pd.Series], Any]
) -> pd.DataFrame:
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

    def _preprocess(self, strings: pd.Series) -> pd.Series:
        strings = strings.astype("string[pyarrow]")
        if self.lower:
            strings = strings.str.lower()
        if self.norm_possesive:
            strings = norm_possessives(strings)
        if self.norm_whitespace:
            strings = norm_whitespace(strings)
        return strings

    def fingerprint(self, data: Data) -> pd.DataFrame:
        """Selects the column of data, preprocesses it, and passes to _func."""
        strings = data[self.column]
        strings = self._preprocess(strings)
        result = self._func(strings)
        non_index = [c for c in result.columns if c != "index"]
        result = result.astype({c: "string[pyarrow]" for c in non_index})
        return result


class TokenFingerprinter(StringBaseFingerprinter):
    def _func(self, strings: pd.Series) -> pd.DataFrame:
        return tokenize(strings).astype({"token": "string[pyarrow]"})


class SortedAcronymFingerprinter(StringBaseFingerprinter):
    def __init__(self, column: str, only_alpha: bool = True) -> None:
        super().__init__(column, lower=True, norm_possesive=True)
        self.only_alpha = only_alpha

    def _func(self, strings: pd.Series) -> pd.DataFrame:
        tokens = tokenize(strings).astype({"token": "string[pyarrow]"})
        tokens = filter_tokens(tokens, lambda token: token.str.len() > 1)
        tokens["first_char"] = tokens["token"].str.slice(0, 1)
        tokens = tokens.drop(columns="token")
        if self.only_alpha:
            tokens = tokens[tokens["first_char"].str.isalpha()]
        tokens = tokens.sort_values(by=["index", "first_char"])
        acronyms = tokens.groupby("index", as_index=False).agg("sum")
        return acronyms
