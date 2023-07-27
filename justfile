# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment
init:
    pdm install -d -G :all
    . .venv/bin/activate

# format code
fmt:
    black .
    ruff --fix .
    nbqa ruff --fix .

# lint code
lint:
    ruff .
    nbqa ruff .
    black -q . --check
    mypy mismo

# run tests
test:
    pytest

# build docs to the site/ directory
docs-build:
    PYDEVD_DISABLE_FILE_VALIDATION=1 mkdocs build

# serve docs for live editing
docs-serve:
    PYDEVD_DISABLE_FILE_VALIDATION=1 mkdocs serve

# publish docs
docs-publish:
    mkdocs gh-deploy --force

# lock dependencies
deps-lock:
    pdm lock -dG :all

# update dependencies
deps-update:
    pdm update -dG :all --update-all  --update-eager
