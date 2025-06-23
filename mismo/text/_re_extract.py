from __future__ import annotations

import re

import ibis.backends.sql.compilers as sc
from ibis.common.annotations import attribute
from ibis.expr import datatypes as dt
from ibis.expr import operations as ops
from ibis.expr import rules as rlz
from ibis.expr import types as ir
import sqlglot.expressions as sge


def re_extract_struct(
    s: ir.StringValue, pattern: str, *, case_insensitive: bool = False
) -> ir.StructValue:
    r"""Use a regex with named groups to extract parts of a string into a struct.

    Examples
    --------
    >>> import ibis
    >>> s = ibis.literal("2024-03-01")
    >>> pattern = r"(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+)"
    >>> re_extract_struct(s, pattern).execute()
    {'year': '2024', 'month': '03', 'day': '01'}
    """
    return RegexExtractStruct(s, pattern, case_insensitive=case_insensitive).to_expr()


class RegexExtractStruct(ops.Value):
    """Use a regex to extract parts of a string into a struct"""

    arg: ops.Value[dt.String]
    pattern: str
    case_insensitive: bool = False

    shape = rlz.shape_like("arg")

    @attribute
    def group_names(self) -> list[str]:
        group_name_pattern = r"\(\?P<([^>]+)>"
        return re.findall(group_name_pattern, self.pattern)

    @attribute
    def dtype(self):
        return dt.Struct.from_tuples([(name, dt.string) for name in self.group_names])


def visit_RegexExtractStruct(self, op: RegexExtractStruct, *, arg, pattern, **kwargs):
    flags = "i" if op.case_insensitive else "c"
    try:
        # Ibis changed their internal API, workaround that here.
        return self.f.regexp_extract(
            arg,
            pattern,
            sge.convert(op.group_names),
            flags,
            dialect=self.dialect,
        )
    except TypeError as e:
        if "got multiple values for keyword argument 'dialect'" not in str(e):
            raise
    return self.f.regexp_extract(
        arg,
        pattern,
        sge.convert(op.group_names),
        flags,
    )


# monkeypatch that sucker in
sc.DuckDBCompiler.visit_RegexExtractStruct = visit_RegexExtractStruct
