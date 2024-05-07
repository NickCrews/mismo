from __future__ import annotations

from typing import Iterable, Literal

import altair as alt
import ibis
from ibis import _
from ibis.expr import types as ir

from mismo._array import array_choice
from mismo._util import get_column
from mismo.block._key_blocker import KeyBlocker


def minhash_lsh_keys(
    terms: ir.ArrayValue, *, band_size: int, n_bands: int
) -> ir.ArrayValue:
    """Create LSH keys from sets of terms."""
    # Many different flavors of how to implement minhash LSH,
    # I chose one based on
    # https://dl.acm.org/doi/10.1145/3511808.3557631
    bands = [array_choice(terms, band_size).sort() for _ in range(n_bands)]
    result = ibis.array(bands).filter(lambda x: x.length() > 0)
    # the .cast() is a workaround for https://github.com/ibis-project/ibis/issues/9135
    result = result.map(lambda x: x.hash().cast("uint64"))
    result = result.unique()
    return result


class MinhashLshBlocker:
    """Uses Minhash LSH to block record pairs that have high Jaccard similarity."""

    def __init__(
        self,
        *,
        terms_column: str,
        band_size: int,
        n_bands: int,
        keys_column: str = "{terms_column}_lsh_keys",
    ):
        self.terms_column = terms_column
        self.band_size = band_size
        self.n_bands = n_bands
        self.keys_column = keys_column

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        *,
        task: Literal["dedupe", "link"] | None = None,
    ) -> ir.Table:
        """Block two tables using Minhash LSH."""
        left_terms = get_column(left, self.terms_column)
        right_terms = get_column(right, self.terms_column)
        keys_name = self.keys_column.format(terms_column=left_terms.get_name())
        left = left.mutate(
            minhash_lsh_keys(
                left_terms, band_size=self.band_size, n_bands=self.n_bands
            ).name(keys_name)
        )
        right = right.mutate(
            minhash_lsh_keys(
                right_terms, band_size=self.band_size, n_bands=self.n_bands
            ).name(keys_name)
        )
        left = left.cache()
        right = right.cache()
        kb = KeyBlocker(_[keys_name].unnest().hash())
        return kb(left, right, task=task)


def plot_lsh_curves(
    band_params: Iterable[tuple[int, int]] | None = None,
) -> alt.Chart:
    """
    Plot performance of Minhash LSH for different band sizes and numbers of bands.

    MinhashLSH is a probabilistic method: The probability `P` that a pair of records
    will be blocked is a function of their jaccard similarity `J`.
    It is in a shape like a logistic curve, a rounded step function:

    ```
        ---------------------------------
    1.0 |                         x x x |
        |                       x       |
        |                      x        |
    P   |                      x        |
        |                      x        |
        |                      x        |
        |                     x         |
    0.0 | x x x x x x x x x x           |
        ---------------------------------
         0.0         Jaccard         1.0
    ```

    The shape of this curve is determined by the band size and number of bands.
    Use this function to choose the best band size and number of bands for your
    use case.

    Based on [this PyData talk](https://youtu.be/n3dCcwWV4_k?si=Q9f4EuGtRE0xSyEg&t=1582)
    and the [accompanying code](https://github.com/mattilyra/LSH/blob/a57069bfb70f4b620d47931f81966b5a73c1b480/examples/Introduction.ipynb)

    Parameters
    ----------
    band_params :
        List of band size and number of bands to plot. If not provided, defaults to
        a reasonable selection to show the various curves.

    Returns
    -------
    The plot of the LSH curve.
    """
    if band_params is None:
        band_params = [
            (2, 10),
            (2, 25),
            (2, 50),
            (2, 100),
            (5, 20),
            (5, 40),
            (10, 10),
            (10, 20),
            (10, 50),
            (20, 5),
            (20, 10),
            (50, 2),
            (50, 4),
        ]
    t = ibis.memtable(data=band_params, columns=["band_size", "n_bands"])
    t = t.mutate(jaccard=ibis.range(51).unnest().cast(float) / 50)
    t = t.mutate(
        label="(" + t.band_size.cast(str) + ", " + t.n_bands.cast(str) + ")",
        pr=1 - (1 - _.jaccard**t.band_size) ** t.n_bands,
    )
    chart = (
        alt.Chart(
            t.to_pandas(),
            title="Probability of LSH blocking a pair given a Jaccard similarity",
            width=400,
            height=400,
        )
        .mark_line(strokeWidth=2, point=True)
        .encode(
            x="jaccard:Q",
            y="pr:Q",
            color=alt.Color(
                "label:N",
                title="Band size, Number of bands",
                sort=alt.EncodingSortField("band_size"),
            ),
            tooltip=["band_size:Q", "n_bands:Q", "jaccard:Q", "pr:Q"],
        )
    )
    return chart
