# How to contribute

## Dependencies

We use [`PDM`](https://pdm-project.org) as our project management tool. Install that [per `PDM`'s instructions](https://pdm-project.org/latest/#recommended-installation-method). On Mac/Linux, you can install it with by running:
```bash
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

As an optional tool, we use [`just`](https://just.systems/man/en/chapter_1.html) and a [justfile](https://github.com/NickCrews/mismo/blob/main/justfile) to automate many of the common development tasks. The rest of this guide will not assume that you have `just` installed.

## Setup dev environemnt

Once `PDM` are installed, run `pdm install -d -G :all`. This will
create a virtual environment in `.venv/` and install all the locked dependencies
in `pdm.lock`.

To enter the venv, use `. .venv/bin/activate`.
You can exit the venv with `deactivate`, as usual.

When you are in the venv, you can run common tasks such as
- Running tests: `pytest`
- Formatting code: `ruff format mismo docs `
- Lint code: `ruff check mismo docs`

See the [justfile](https://github.com/NickCrews/mismo/blob/main/justfile)
for more tasks.
