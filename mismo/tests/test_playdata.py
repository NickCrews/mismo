from __future__ import annotations

from typing import Callable

import ibis
import pytest

from mismo.linkage._linkage import Linkage
from mismo.playdata import (
    load_febrl1,
    load_febrl2,
    load_febrl3,
    load_patents,
    load_rldata500,
    load_rldata10000,
)


@pytest.mark.parametrize(
    "load_func, expected_count, expected_link_count",
    [
        (load_febrl1, 1000, 500),
        (load_febrl2, 5000, 1934),
        (load_febrl3, 5000, 6538),
    ],
)
def test_load_febrl_smoketest(
    load_func: Callable[..., Linkage], expected_count, expected_link_count
):
    linkage = load_func()
    assert linkage.left.count().execute() == expected_count
    assert linkage.right.count().execute() == expected_count
    assert linkage.links.count().execute() == expected_link_count
    repr(linkage)


def test_load_patents_smoketest():
    linkage = load_patents()
    assert linkage.left.count().execute() == 2379
    repr(linkage)


class TestRLData:
    EXPECTED_SCHEMA = {
        "record_id": "int64",
        "label_true": "int64",
        "fname_c1": "string",
        "fname_c2": "string",
        "lname_c1": "string",
        "lname_c2": "string",
        "by": "int64",
        "bm": "int64",
        "bd": "int64",
    }

    def test_load_rldata500(self):
        linkage = load_rldata500()
        records = linkage.left

        assert records.schema().equals(ibis.schema(TestRLData.EXPECTED_SCHEMA))
        assert records.count().execute() == 500
        assert records["label_true"].nunique().execute() == 450  # 50 duplicates

        # Proportion of clusters where surname is not unique.
        # Should be around 3.5%, i.e. around one third of the
        # 11% of clusters with more than one record.
        name_variation_rate = 1 - (
            records.aggregate(
                by="label_true", unique_surname=records["lname_c1"].nunique() == 1
            )["unique_surname"].mean()
        )
        assert name_variation_rate.execute() == 0.03555555555555556

    def test_load_rldata10000(self):
        linkage = load_rldata10000()
        records = linkage.left

        assert records.schema().equals(ibis.schema(TestRLData.EXPECTED_SCHEMA))
        assert records.count().execute() == 10000
        assert records["label_true"].nunique().execute() == 9000  # 9000 duplicates

        name_variation_rate = 1 - (
            records.aggregate(
                by="label_true", unique_surname=records["lname_c1"].nunique() == 1
            )["unique_surname"].mean()
        )
        assert name_variation_rate.execute() == 0.030444444444444496
