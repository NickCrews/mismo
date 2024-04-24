# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment (but don't activate it)
install:
    pdm install -d -G :all

# format code
fmt:
    ruff check --fix mismo docs
    ruff format mismo docs

# lint code
lint:
    ruff check mismo docs
    ruff format --check mismo docs

# run tests
test *FILES:
    pytest {{FILES}}

# include --dev-addr localhost:8001 to avoid conflicts with other mkdocs instances
# serve docs for live editing
docs:
    PYDEVD_DISABLE_FILE_VALIDATION=1 mkdocs serve --dev-addr localhost:8001

# build docs to the site/ directory
docs-build:
    PYDEVD_DISABLE_FILE_VALIDATION=1 mkdocs build

# publish docs
docs-publish:
    mkdocs gh-deploy --force

# run the timing benchmark suite
bench:
    pytest --benchmark-only --benchmark-enable --benchmark-autosave

# run timing benchmarks and compare with a previous run
benchcmp number *args:
    just bench --benchmark-compare {{ number }} {{ args }}

# update dependencies
deps-update:
    pdm update -dG :all --update-all
