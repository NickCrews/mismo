# Mismo

[![PyPI - Version](https://img.shields.io/pypi/v/mismo.svg)](https://pypi.org/project/mismo)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mismo.svg)](https://pypi.org/project/mismo)

The SQL/Ibis powered sklearn of record linkage.

-----

## Installation

```console
pip install mismo
```

## Goals

Mismo tries to be the sklearn of record linkage, backed by the scalability
and power of SQL and Ibis. It is made of many small
data structures and functions, each with a well-defined API that allows them
to be composed together and extended easily. None of the other record linkage
packages I have seen, such as
[Splink](https://github.com/moj-analytical-services/splink),
[Dedupe](https://www.github.com/dedupeio/dedupe), or
[Record Linkage Toolkit](https://github.com/J535D165/recordlinkage),
had all of these properties, so I decided to make my own.

See [Goals and Alternatives](https://nickcrews.github.io/mismo/concepts/goals_and_alternatives)
for a more detailed discussion of the goals of Mismo and how it compares to other
record linkage packages.

## Features
- Supports very large datasets that are larger than memory, executed on
  powerful SQL engines.
- Use the clean, strong-typed, pythonic, and Dataframe API of Ibis.
- Small, modular functions and data structures that are easy to plug together
  and extend. Ibis supports UDFs and vectorized UDFs, so you can easily
  extend Mismo with your own custom functions.

## License

`mismo` is distributed under the terms of the
[LGPL-3.0-or-later](https://spdx.org/licenses/LGPL-3.0-or-later.html) license.
