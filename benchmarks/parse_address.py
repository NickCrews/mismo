import datetime
import os
from pathlib import Path
from time import time
from typing import Callable

import ibis
import ibis.expr.types as ir
import pandas as pd
from ibis import _, Table

from mismo.lib.geo import postal_parse_address
from mismo.lib.geo.tests.test_postal_benchmark import noop, postal_only, postal_parse_address__direct_import, postal_parse_address__initial_impl, python_only


_CURRENT_DIR = Path(__file__).parent
_DB_DIR = Path(_CURRENT_DIR, 'db')


def _prepare_db_table(benchmark_id: str, db_name: str) -> Table:
    apoc_file = Path(_CURRENT_DIR, 'apoc_addresses_1M.parquet')
    apoc_data = pd.read_parquet(apoc_file)

    db_file = Path(_DB_DIR, benchmark_id, db_name)
    os.makedirs(db_file.parent, exist_ok=True)
    con = ibis.duckdb.connect(db_file)
    t = con.create_table(db_name, apoc_data)

    return t


def run_benchmark(benchmark_id: str, parse_fn: Callable[..., ir.Value]) -> None:
    input_table = _prepare_db_table(benchmark_id, f"{parse_fn.__name__}.ddb")
    input_table = input_table.cache()
    start = time()
    res = parse_fn(input_table.full_address)
    persisted = res.as_table().cache()
    end = time()
    print(f"{parse_fn.__name__:<35} took {end - start:>8.4f} seconds")


def main():
    # Windows does not allow ':' in file names
    benchmark_id = datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")

    run_benchmark(benchmark_id, noop)
    run_benchmark(benchmark_id, python_only)
    run_benchmark(benchmark_id, postal_only)
    run_benchmark(benchmark_id, postal_parse_address)
    run_benchmark(benchmark_id, postal_parse_address__direct_import)
    run_benchmark(benchmark_id, postal_parse_address__initial_impl)


if __name__ == '__main__':
    main()
