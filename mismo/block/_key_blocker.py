from __future__ import annotations

from typing import Callable, Literal

from ibis import Deferred
from ibis.expr import types as ir

from mismo import _util
from mismo.block._core import block_on_id_pairs, join


class KeyBlocker:
    """Blocks records together wherever they share a key, eg "emails match."

    This is one of the most basic blocking rules, used very often in record linkage.
    This is what is used in `splink`.

    Examples
    --------
    >>> import ibis
    >>> from ibis import _
    >>> import mismo
    >>> ibis.options.interactive = True
    >>> t = mismo.datasets.load_patents()["record_id", "name", "latitude"]
    >>> t.head()
    ┏━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┓
    ┃ record_id ┃ name                         ┃ latitude ┃
    ┡━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━┩
    │ int64     │ string                       │ float64  │
    ├───────────┼──────────────────────────────┼──────────┤
    │      2909 │ * AGILENT TECHNOLOGIES, INC. │     0.00 │
    │      3574 │ * AKZO NOBEL N.V.            │     0.00 │
    │      3575 │ * AKZO NOBEL NV              │     0.00 │
    │      3779 │ * ALCATEL N.V.               │    52.35 │
    │      3780 │ * ALCATEL N.V.               │    52.35 │
    └───────────┴──────────────────────────────┴──────────┘

    Block the table with itself wherever the names match:

    >>> blocker = mismo.block.KeyBlocker("name")
    >>> blocker(t, t).head()
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l               ┃ name_r               ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string               │ string               │
    ├─────────────┼─────────────┼────────────┼────────────┼──────────────────────┼──────────────────────┤
    │      665768 │      665769 │  51.683333 │        0.0 │ ALCOA NEDERLAND B.V. │ ALCOA NEDERLAND B.V. │
    │     1598894 │     1598895 │  51.416667 │        0.0 │ ASML NETHERLAND B.V. │ ASML NETHERLAND B.V. │
    │     4332214 │     4332215 │  52.350000 │        0.0 │ Canon Europa N.V.    │ Canon Europa N.V.    │
    │     7651166 │     7651167 │  50.900000 │        0.0 │ DSM B.V.             │ DSM B.V.             │
    │     7651339 │     7651340 │  50.900000 │       50.9 │ DSM I.P. Assets B.V. │ DSM I.P. Assets B.V. │
    └─────────────┴─────────────┴────────────┴────────────┴──────────────────────┴──────────────────────┘

    Arbitrary blocking keys are supported. For example, block the table wherever
        - the first 5 characters of the name in uppercase, are the same
          AND
        - the latitudes, rounded to 1 decimal place, are the same

    >>> blocker = mismo.block.KeyBlocker((_["name"][:5].upper(), _.latitude.round(1)))
    >>> blocker(t, t).head()
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                ┃ name_r                     ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                │ string                     │
    ├─────────────┼─────────────┼────────────┼────────────┼───────────────────────┼────────────────────────────┤
    │        3574 │        3575 │   0.000000 │   0.000000 │ * AKZO NOBEL N.V.     │ * AKZO NOBEL NV            │
    │      663246 │      663255 │  52.016667 │  52.025498 │ Alcatel NV            │ ALCATEL N.V., RIJSWIJK, NL │
    │      665768 │      665773 │  51.683333 │  51.683333 │ ALCOA NEDERLAND B.V.  │ Alcoa Nederland B.V.       │
    │     1598972 │     1598988 │  51.416667 │  51.416667 │ Asml Netherlands B.V. │ ASML Netherlands-B.V.      │
    │     7651427 │     7651428 │  50.900000 │  50.900000 │ DSM IP assets B.V.    │ DSM Ip Assets B.V.         │
    └─────────────┴─────────────┴────────────┴────────────┴───────────────────────┴────────────────────────────┘

    We can even block on arrays! For example, first let's split each name into
    significant tokens:

    >>> tokens = _.name.upper().split(" ").filter(lambda x: x.length() > 4)
    >>> tokens.resolve(t)
    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ ArrayFilter(StringSplit(Uppercase(name), ' '), Greater(StringLength(x), 4)) ┃
    ┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ array<string>                                                               │
    ├─────────────────────────────────────────────────────────────────────────────┤
    │ ['AGILENT', 'TECHNOLOGIES,']                                                │
    │ ['NOBEL']                                                                   │
    │ ['NOBEL']                                                                   │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['ALCATEL']                                                                 │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ ['CANON', 'EUROPA']                                                         │
    │ []                                                                          │
    │ …                                                                           │
    └─────────────────────────────────────────────────────────────────────────────┘

    Now, block the tables together wherever two records share a token.
    Note that this blocked `* SCHLUMBERGER LIMITED` with `* SCHLUMBERGER TECHNOLOGY BV`.

    >>> blocker = mismo.block.KeyBlocker(tokens.unnest())
    >>> blocker(t, t).filter(_.name_l != _.name_r)
    ┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ record_id_l ┃ record_id_r ┃ latitude_l ┃ latitude_r ┃ name_l                                            ┃ name_r                                            ┃
    ┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
    │ int64       │ int64       │ float64    │ float64    │ string                                            │ string                                            │
    ├─────────────┼─────────────┼────────────┼────────────┼───────────────────────────────────────────────────┼───────────────────────────────────────────────────┤
    │        3574 │        3575 │   0.000000 │   0.000000 │ * AKZO NOBEL N.V.                                 │ * AKZO NOBEL NV                                   │
    │       62445 │       66329 │   0.000000 │   0.000000 │ * N V PHILIPS' GLOEILAMPENFABRIEKEN               │ * N.V. PHILIPS' GLOEILAMPENFABRIEKEN              │
    │       79860 │       79872 │  52.500000 │   0.000000 │ * SCHLUMBERGER LIMITED                            │ * SCHLUMBERGER TECHNOLOGY BV                      │
    │       81613 │       81633 │  52.083333 │  52.083333 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ B.V. │
    │       81631 │       81641 │  52.500000 │  52.083333 │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ B.V. │ * SHELL INTERNATIONALE RESEARCH MAATSCHAPPIJ BV   │
    │       81614 │      317966 │   0.000000 │  52.350000 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ Adidas International Marketing B.V.               │
    │       81614 │      317969 │   0.000000 │  52.350000 │ * SHELL INTERNATIONAL RESEARCH MAATSCHHAPPIJ B.V. │ adidas International Marketing B.V,               │
    │      317969 │      317971 │  52.350000 │  52.500000 │ adidas International Marketing B.V,               │ adidas International Marketing B.V.               │
    │      317967 │      317971 │   0.000000 │  52.500000 │ Adidas International Marketing B.V.               │ adidas International Marketing B.V.               │
    │      317968 │      317971 │  52.350000 │  52.500000 │ adidas International Marketing, B.V.              │ adidas International Marketing B.V.               │
    │           … │           … │          … │          … │ …                                                 │ …                                                 │
    └─────────────┴─────────────┴────────────┴────────────┴───────────────────────────────────────────────────┴───────────────────────────────────────────────────┘
    """  # noqa: E501

    def __init__(
        self,
        key: str
        | Deferred
        | Callable[[ir.Table], ir.Column | tuple[ir.Column, ir.Column]]
        | tuple[str | Deferred, str | Deferred],
        *,
        name: str | None = None,
    ) -> None:
        """Create a new key blocker.

        Parameters
        ----------
        key
            The key to block on. This can be any of the following:

            - A string, which is interpreted as the name of a column in both tables.
              eg "price" is equivalent to `left.price == right.price`
            - A Deferred, which is used to reference a column in a table.
              eg `_.price.fillna(0)` is equivalent to
              `left.price.fillna(0) == right.price.fillna(0)`
            - An iterable of the above, which is interpreted as a tuple of conditions.
              eg `("age", _.first_name.upper()")` is equivalent to
              `(left.age == right.age) & (left.first_name.upper() == right.first_name.upper())`
        """  # noqa: E501
        self.key = key
        self._name = name if name is not None else _util.get_name(self.key)

    @property
    def name(self) -> str:
        """The name of the KeyBlocker."""
        return self._name

    def __call__(
        self,
        left: ir.Table,
        right: ir.Table,
        task: Literal["dedupe", "link"] | None = None,
        **kwargs,
    ):
        j = join(left, right, self.key, task=task, on_slow="ignore", **kwargs)
        id_pairs = j.select("record_id_l", "record_id_r").distinct()
        return block_on_id_pairs(left, right, id_pairs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"
