# Mismo

[![PyPI - Version](https://img.shields.io/pypi/v/mismo.svg)](https://pypi.org/project/mismo)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/mismo.svg)](https://pypi.org/project/mismo)

-----

**Table of Contents**

- [Installation](#installation)
- [Goals](#goals)
- [License](#license)

## Installation

```console
pip install mismo
```

## Goals

### Use [Ibis](https://ibis-project.org/) as the core
This gives a few benefits that are key to record linkage:
- Ability to use datasets that are larger than memory
- Ability to use multiple backends (eg `duckdb` for single node,
  or `bigquery` or `spark` for distributed)

### Thoughtful, composable API

Use a duck-typing approach to allow users to plug in their own components
eg "Blocker" has a `block` method with a certain signature.
This makes mismo a bit more complicated than `dedupe` or `splink`, but
it will be much more flexible.

### Extras
- More ergonomic model persistence than `dedupe`. `splink` did a good job here.
- Support determinism using `random_state` (unlike `dedupe`)

## License

`mismo` is distributed under the terms of the [LGPL-3.0-or-later](https://spdx.org/licenses/LGPL-3.0-or-later.html) license.
