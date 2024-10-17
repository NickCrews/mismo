# How to contribute

## Dependencies

We use [`uv`](https://github.com/astral-sh/uv) as our package and dependency management tool. Install that 
[per uv's instructions](https://docs.astral.sh/uv/getting-started/installation/). 
As of this writing, this is done by running:
```bash
curl -LsSf https://astral.sh/uv/0.4.18/install.sh | sh
```

We use [`just`](https://just.systems/man/en/chapter_1.html) and a 
[justfile](https://github.com/NickCrews/mismo/blob/main/justfile) 
to automate many of the common development tasks. The rest of this guide will assume
you have just installed. If you don't want to use `just`, you can manually run recipes from the 
[justfile](https://github.com/NickCrews/mismo/blob/main/justfile).

## Setup dev environemnt

Once `uv` (and optionally `just`) are installed, run `just install`. This will
create a virtual environment in `.venv/` and install all the locked dependencies
from `uv.lock`.

Now you can run common tasks such as
- `just test`
- `just fmt`
- `just lint`