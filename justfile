# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment (but don't activate it)
install:
    uv sync --all-extras

# format code
fmt:
    uv run ruff check --fix mismo docs
    uv run ruff format mismo docs

# lint code
lint:
    uv run ruff check mismo docs
    uv run ruff format --check mismo docs

# run tests
test *FILES:
    uv run pytest {{FILES}}

# include --dev-addr localhost:8001 to avoid conflicts with other mkdocs instances
# serve docs for live editing
docs:
    PYDEVD_DISABLE_FILE_VALIDATION=1 uv run mkdocs serve --dev-addr localhost:8001

# build docs to the site/ directory
docs-build:
    PYDEVD_DISABLE_FILE_VALIDATION=1 uv run mkdocs build

# publish docs
docs-publish:
    uv run mkdocs gh-deploy --force

# run the timing benchmark suite
# just bench -k test_benchmark_us_census_geocode[100]
bench *args:
    uv run pytest --benchmark-only --benchmark-enable --benchmark-autosave --benchmark-group-by=func {{args}}

benchmark *args:
    uv run bench {{args}}

# run timing benchmarks and compare with a previous run
benchcmp number *args:
    just bench --benchmark-compare {{ number }} {{ args }}

# update dependencies
update:
    uv lock --update

#install libpostal to the system, a dependency for the pypostal python package 
install-libpostal datadir="/tmp/postal":
    #!/usr/bin/env bash
    if [ "$(uname)" = "Linux" ]; then sudo apt-get install curl autoconf automake libtool pkg-config; fi
    if [ "$(uname)" = "Darwin" ]; then brew install curl autoconf automake libtool pkg-config; fi
    git clone https://github.com/openvenues/libpostal
    cd libpostal
    ./bootstrap.sh
    if [ "$(uname)" = "Linux" ]; then ./configure --datadir={{datadir}}; fi
    if [ "$(uname)" = "Darwin" ]; then ./configure --datadir={{datadir}} --disable-sse2; fi
    make -j4
    sudo make install
    if [ "$(uname)" = "Linux" ]; then sudo ldconfig; fi
    cd ..
    rm -rf libpostal