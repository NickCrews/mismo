# Mismo

[![PyPI - Version](https://img.shields.io/pypi/v/mismo.svg)](https://pypi.org/project/mismo)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mismo.svg)](https://pypi.org/project/mismo)

The SQL/Ibis powered sklearn of record linkage.

Still in alpha stage. Breaking changes will happen frequently
and with no warning. Once things are more stabilized I
will come up with a stability policy. Any suggestions as
to how you want the API to look like would be greatly appreciated.
I do use this in my work, so at least I do decent job of
ensuring correctness.

-----

## Goals

Mismo tries to be the sklearn of record linkage, backed by the scalability
and power of SQL and [Ibis](https://ibis-project.org/). It is made of many small
data structures and functions, each with a well-defined and standard API
that allows them to be composed together and extended easily.
None of the other record linkage packages I have seen, such as
[Splink](https://github.com/moj-analytical-services/splink),
[Dedupe](https://www.github.com/dedupeio/dedupe), or
[Record Linkage Toolkit](https://github.com/J535D165/recordlinkage),
had all of these properties, so I decided to make my own.

See [Goals and Alternatives](https://nickcrews.github.io/mismo/concepts/goals_and_alternatives)
for a more detailed discussion of the goals of Mismo and how it compares to other
record linkage packages.

## Features
- Supports larger-than-memory datasets, executed on powerful SQL engines.
  Use DuckDB for prototyping and for jobs up to maybe ~10M records,
  or Spark or other distributed backends for larger tasks, without
  needing to change your code!
- Use the clean, strong-typed, pythonic, Dataframe APIs of [Ibis](https://ibis-project.org/).
- Small, modular functions and data structures that are easy to plug together
  and extend.
- Layered API: Use top-level APIs if your task is common enough that it is
  supported out of the box.

## Installation

[`mismo` is available on PyPI](https://pypi.org/project/mismo/).
I try to publish semver'ed releases after most changes.

If I forget to do this, then there are also[prereleases on PyPI](https://pypi.org/project/mismo/#history).
These are published every week by a github action using the HEAD commit of this repo.

You can also install directly from a branch or a specific commit from github:

```console
uv pip install "mismo[viz] @ git+https://github.com/NickCrews/mismo@<SOME-SHA-OR-BRANCH>"
```

## Examples

See the [example notebook](https://nickcrews.github.io/mismo/examples/patent_deduplication).

## Documentation

See the [documentation](https://nickcrews.github.io/mismo).

## Contributing

See the [contributing guide](https://nickcrews.github.io/mismo/contributing/).

## License

`mismo` is distributed under the terms of the
[LGPL-3.0-or-later](https://spdx.org/licenses/LGPL-3.0-or-later.html) license.
