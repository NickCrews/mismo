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
# just bench -k test_benchmark_us_census_geocode[100]
bench *args:
    pytest --benchmark-only --benchmark-enable --benchmark-autosave {{args}}

benchmark *args:
    bench {{args}}

# run timing benchmarks and compare with a previous run
benchcmp number *args:
    just bench --benchmark-compare {{ number }} {{ args }}

# update dependencies
update:
    pdm update -dG :all --update-all

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