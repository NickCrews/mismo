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

- More performant than `dedupe` by using vectorized operations
  (e.g. `numpy` instead of operating on individual records)
- Works with larger-than-memory datasets (using `modin`)
- Consistent, duck-typing-based API similar to `sklearn`
  (eg a "Blocker" has a `block` method with a certain signature)
- More intuitive model persistence than `dedupe`
- Support determinism using `random_state` (unlike `dedupe`)

## License

`mismo` is distributed under the terms of the [LGPL-3.0-or-later](https://spdx.org/licenses/LGPL-3.0-or-later.html) license.
