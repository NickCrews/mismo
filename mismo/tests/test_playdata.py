from __future__ import annotations

import ibis
import pytest

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
def test_load_febrl_smoketest(load_func, expected_count, expected_link_count):
    data, links = load_func()
    assert data.count().execute() == expected_count
    assert links.count().execute() == expected_link_count
    repr(data)


def test_load_patents_smoketest():
    dataset = load_patents()
    assert dataset.count().execute() == 2379
    repr(dataset)


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
        dataset = load_rldata500()

        assert dataset.schema().equals(ibis.schema(TestRLData.EXPECTED_SCHEMA))
        assert dataset.count().execute() == 500
        assert dataset["label_true"].nunique().execute() == 450  # 50 duplicates

        # Proportion of clusters where surname is not unique.
        # Should be around 3.5%, i.e. around one third of the
        # 11% of clusters with more than one record.
        name_variation_rate = 1 - (
            dataset.aggregate(
                by="label_true", unique_surname=dataset["lname_c1"].nunique() == 1
            )["unique_surname"].mean()
        )
        assert name_variation_rate.execute() == 0.03555555555555556

    def test_load_rldata10000(self):
        dataset = load_rldata10000()

        assert dataset.schema().equals(ibis.schema(TestRLData.EXPECTED_SCHEMA))
        assert dataset.count().execute() == 10000
        assert dataset["label_true"].nunique().execute() == 9000  # 9000 duplicates

        name_variation_rate = 1 - (
            dataset.aggregate(
                by="label_true", unique_surname=dataset["lname_c1"].nunique() == 1
            )["unique_surname"].mean()
        )
        assert name_variation_rate.execute() == 0.030444444444444496
