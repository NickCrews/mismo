from __future__ import annotations

import pytest

from mismo.datasets import (
    load_cen_msr,
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
    EXPECTED_COLS = {
        "record_id",
        "fname_c1",
        "fname_c2",
        "lname_c1",
        "lname_c2",
        "by",
        "bm",
        "bd",
        "label_true",
    }

    def test_load_rldata500(self):
        dataset = load_rldata500()

        assert set(dataset.columns) == TestRLData.EXPECTED_COLS
        assert dataset.count().execute() == 500
        assert dataset["label_true"].nunique().execute() == 450  # 50 duplicates

        # Proportion of clusters where last name is not unique.
        # Should be around 3.5%, i.e. around one third of the
        # 11% of clusters with more than one record.
        name_variation_rate = 1 - (
            dataset.aggregate(
                by="label_true", unique_last_name=dataset["lname_c1"].nunique() == 1
            )["unique_last_name"].mean()
        )
        assert name_variation_rate.execute() == 0.03555555555555556

    def test_load_rldata10000(self):
        dataset = load_rldata10000()

        assert set(dataset.columns) == TestRLData.EXPECTED_COLS
        assert dataset.count().execute() == 10000
        assert dataset["label_true"].nunique().execute() == 9000  # 9000 duplicates

        name_variation_rate = 1 - (
            dataset.aggregate(
                by="label_true", unique_last_name=dataset["lname_c1"].nunique() == 1
            )["unique_last_name"].mean()
        )
        assert name_variation_rate.execute() == 0.030444444444444496


def test_load_cen_msr():
    cen, msr = load_cen_msr()

    CEN_COLUMNS = [
        "record_id",
        "label_true",
        "first_name",
        "middle_name",
        "last_name",
        "birth_year",
        "birth_month",
        "birth_place",
        "gender",
    ]
    MSR_COLUMNS = [
        "record_id",
        "label_true",
        "first_name",
        "middle_name",
        "last_name",
        "birth_date",
        "birth_place",
        "enlist_date",
        "enlist_age",
    ]

    assert cen.columns == CEN_COLUMNS
    assert cen.count().execute() == 54752

    assert msr.columns == MSR_COLUMNS
    assert msr.count().execute() == 39340
