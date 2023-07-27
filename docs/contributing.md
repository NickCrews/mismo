# How to contribute

## Dependencies

We use `PDM` as our project management tool. Install that per `PDM`'s instructions.

Then, this is not necessary, but it makes things easier, we use `just` to automate
many of the common development tasks. The rest of this guide will assume
you have just installed, but if you don't want to use `just`, then you
can run all the recipes manually, just look at the
[justfile](https://github.com/NickCrews/mismo/blob/main/justfile)
for the relevant recipe.

## Setup dev environemnt

Once `PDM` (and optionally `just`) are installed, run `just init`. This will
create a virtual environment in `.venv/`, install all the locked dependencies
in `pdm.lock`, and activate the venv. You can exit the venv with `deactivate`,
as usual.

When you are in the venv, you can run common tasks such as
- `just test`
- `just fmt`
- `just lint`

See the [justfile](https://github.com/NickCrews/mismo/blob/main/justfile)
for more recipes.
