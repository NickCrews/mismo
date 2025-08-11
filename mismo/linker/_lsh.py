from __future__ import annotations

from typing import TYPE_CHECKING, Iterable

import ibis
from ibis import _
from ibis.expr import types as ir

from mismo._util import get_column
from mismo.arrays import array_choice
from mismo.linker import _common

if TYPE_CHECKING:
    import altair as alt


def minhash_lsh_keys(
    terms: ir.ArrayValue, *, band_size: int, n_bands: int
) -> ir.ArrayValue:
    """Create LSH keys from sets of terms."""
    # Many different flavors of how to implement minhash LSH,
    # I chose one based on
    # https://dl.acm.org/doi/10.1145/3511808.3557631
    bands = [array_choice(terms, band_size).sort() for _ in range(n_bands)]
    result = ibis.array(bands).filter(lambda x: x.length() > 0)

    def _hash(x):
        # NOTE: returns int64 in ibis 9.1.0 and later but uint64 in earlier versions
        if ibis.__version__ >= "9.1.0":
            # in later ibis versions, there is some post-processing
            # so the uint64s that duckdb returns are properly cast to int64s
            return x.hash()
        else:
            # in earlier versions, ibis *says* it returns int64s,
            # but duckdb actually returns uint64s, which
            # crashes things down the line without this cast.
            # See https://github.com/ibis-project/ibis/issues/9135
            # and https://github.com/NickCrews/mismo/issues/45
            return x.hash().cast("uint64")

    result = result.map(_hash)
    result = result.unique()
    return result


class MinhashLshLinker(_common.Linker):
    """A [Linker][mismo.Linker] that uses Minhash LSH to block record pairs that have high Jaccard similarity.

    See [the how-to guide](../howto/lsh.ipynb) for more information.
    """  # noqa: E501

    def __init__(
        self,
        *,
        terms_column: str,
        band_size: int,
        n_bands: int,
        keys_column: str = "{terms_column}_lsh_keys",
    ) -> None:
        """Make a Minhash LSH blocker.

        Parameters
        ----------
        terms_column :
            The column that holds the terms to compare.
        band_size :
            The number of terms in each band.
            See [plot_lsh_curves][mismo.block.plot_lsh_curves] for guidance.
        n_bands :
            The number of bands.
            See [plot_lsh_curves][mismo.block.plot_lsh_curves] for guidance.
        keys_column :
            The name of the column that will hold the LSH keys.
        """
        self.terms_column = terms_column
        self.band_size = band_size
        self.n_bands = n_bands
        self.keys_column = keys_column

    def __call__(self, left: ir.Table, right: ir.Table) -> ir.Table:
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
        # kb = KeyBlocker(_[keys_name].unnest())
        # return kb(left, right, task=task)


def p_blocked(jaccard: float, band_size: int, n_bands: int) -> float:
    return 1 - (1 - jaccard**band_size) ** n_bands


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

    ![](../assets/lsh_curves.png)

    The shape of this curve is determined by the band size and number of bands.
    Use this function to choose the best band size and number of bands for your
    use case. Note that the runtime of the Minhash LSH blocker runs in
    `O(band_size * n_bands * n_records)` time, so you want to choose as small
    of band size and number of bands as possible.

    Based on [this PyData talk](https://youtu.be/n3dCcwWV4_k?si=Q9f4EuGtRE0xSyEg&t=1582)
    and the [accompanying code](https://github.com/mattilyra/LSH/blob/a57069bfb70f4b620d47931f81966b5a73c1b480/examples/Introduction.ipynb)

    Parameters
    ----------
    band_params :
        List of (band size, number of bands) to plot. If not provided, defaults to
        a reasonable selection to show the various curves.

    Returns
    -------
    The plot of the LSH curve.
    """
    import altair as alt

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
        pr=p_blocked(_.jaccard, _.band_size, _.n_bands),
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
