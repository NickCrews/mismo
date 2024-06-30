# How to contribute

## Dependencies

We use [`PDM`](https://pdm-project.org) as our package and dependency management tool. Install that 
[per `PDM`'s instructions](https://pdm-project.org/latest/#recommended-installation-method). 
On Mac/Linux, this is done by running:
```bash
curl -sSL https://pdm-project.org/install-pdm.py | python3 -
```

We use [`just`](https://just.systems/man/en/chapter_1.html) and a 
[justfile](https://github.com/NickCrews/mismo/blob/main/justfile) 
to automate many of the common development tasks. The rest of this guide will assume
you have just installed. If you don't want to use `just`, you can manually run recipes from the 
[justfile](https://github.com/NickCrews/mismo/blob/main/justfile).

## Setup dev environemnt

Once `PDM` (and optionally `just`) are installed, run `just init`. This will
create a virtual environment in `.venv/`, install all the locked dependencies
in `pdm.lock`.

To enter the venv, use `. .venv/bin/activate`.
You can exit the venv with `deactivate`, as usual.

When you are in the venv, you can run common tasks such as
- `just test`
- `just fmt`
- `just lint`

See the [justfile](https://github.com/NickCrews/mismo/blob/main/justfile)
for more recipes.
