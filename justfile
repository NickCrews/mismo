# Copied from https://github.com/ibis-project/ibis/blob/master/justfile

# list justfile recipes
default:
    just --list

# initialize development environment (but don't activate it)
install:
    uv sync --all-extras --group dev

# format code
fmt:
    uv run --group lint ruff check --fix mismo docs
    uv run --group lint ruff format mismo docs

# lint code
lint:
    uv run --group lint ruff check mismo docs
    uv run --group lint ruff format --check mismo docs

# run tests
test *FILES:
    #!/usr/bin/env bash
    # In windows github actions, the uname command returns "MSYS_NT-10.0-20348"
    # Search for "MSYS_NT"
    if [ "$(uname | tr '[:upper:]' '[:lower:]' | grep 'msys_nt')" ]; then
        uv run --group test --all-extras pytest {{FILES}}
    else
        uv run --group test --all-extras pytest --doctest-modules {{FILES}}
    fi

# include --dev-addr localhost:8001 to avoid conflicts with other mkdocs instances
# serve docs for live editing
docs:
    PYDEVD_DISABLE_FILE_VALIDATION=1 uv run --group docs mkdocs serve --dev-addr localhost:8001

# build docs to the site/ directory
docs-build:
    PYDEVD_DISABLE_FILE_VALIDATION=1 uv run --group docs mkdocs build

# publish docs
docs-publish:
    uv run --group docs mkdocs gh-deploy --force

# run the timing benchmark suite
# just bench -k test_benchmark_us_census_geocode[100]
bench *args:
    uv run --group test pytest --benchmark-only --benchmark-enable --benchmark-autosave --benchmark-group-by=func {{args}}

# run timing benchmarks and compare with a previous run
benchcmp number *args:
    just bench --benchmark-compare {{ number }} {{ args }}

# upgrade dependencies
upgrade:
    uv lock --upgrade

# compute the next dev version number, eg 0.0.1.dev823
# Copied from https://github.com/ibis-project/ibis/commit/9e602af3d4847e9ce112045ba11248b7770931fc
@compute-dev-version:
    #!/usr/bin/env -S uv run --script
    # /// script
    # requires-python = ">=3.11"
    # dependencies=["dunamai==1.22.0"]
    # ///
    from dunamai import Version
    version = Version.from_git(latest_tag=True, pattern="default-unprefixed")
    if version.distance:
        version = version.bump(index=-1)
        format = "{base}.dev{distance}"
    else:
        format = None
    print(version.serialize(format=format))

# allow setting a specific version, or compute dev version if not provided
set-version vsn='':
    #!/usr/bin/env bash

    version={{vsn}}
    if [ -z "$version" ]; then
        version="$(just compute-dev-version)"
    fi
    uvx --from=toml-cli toml set --toml-path=pyproject.toml project.version "$version" > /dev/null
    uv lock > /dev/null
    echo "$version"

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