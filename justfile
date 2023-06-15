# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment
init:
    pdm install
    source .venv/bin/activate

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
    mypy .

# run tests
test:
    pytest
