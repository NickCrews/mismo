# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment (but don't activate it)
install:
    pdm install -d -G :all

# format code
fmt:
    ruff --fix mismo docs
    ruff format mismo docs

# lint code
lint:
    ruff mismo docs
    ruff format --check mismo docs

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

# update dependencies
deps-update:
    pdm update -dG :all --update-all
