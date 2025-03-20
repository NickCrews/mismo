from __future__ import annotations

import pytest

from mismo.linkage import sample_all_links


@pytest.mark.parametrize(
    "n_records,max_pairs",
    [
        (10_000, 0),
        (10_000, 1),
        (10_000, 2),
        (10_000, 10),
        (10_000, 100_000),
        # Stress test the implementation we
        # use to ensure that pairs are not duplicated.
        (2, 3),
        (10, 99),
        # Edge case where n_possible_pairs < max_pairs
        (3, 10_000_000),
    ],
)
def test_sample_all_pairs(table_factory, n_records: int, max_pairs: int):
    t = table_factory(
        {
            "record_id": range(n_records),
            # Something to check that the column is not included in output
            "value": [i // 7 for i in range(n_records)],
        }
    )
    df = sample_all_links(t, t, max_pairs=max_pairs).execute()
    expected_pairs = min(n_records**2, max_pairs)
    assert df.columns.tolist() == ["record_id_l", "record_id_r"]
    assert len(df) == expected_pairs
    assert df.record_id_l.notnull().all()
    assert df.record_id_r.notnull().all()
    # We get no duplicates
    assert len(df.drop_duplicates()) == expected_pairs

    # Only do these stats if we have a reasonable number of pairs
    if expected_pairs >= 1000:
        # These two aren't correlated
        assert (df.record_id_l == df.record_id_r).mean() < 0.01
        # We get about an even distribution, the head or tail are not overrepresented
        # We expect the mean of record ids to be about half of the n_pairs
        expected = n_records / 2
        pct_err_l = abs(df.record_id_l.mean() - expected) / expected
        pct_err_r = abs(df.record_id_r.mean() - expected) / expected
        assert pct_err_l < 0.01
        assert pct_err_r < 0.01


@pytest.mark.parametrize("max_pairs", [None, 0, 1, 100_000])
def test_sample_all_pairs_empty(table_factory, max_pairs: int | None):
    t = table_factory({"record_id": []})
    df = sample_all_links(t, t, max_pairs=max_pairs).execute()
    assert df.columns.tolist() == ["record_id_l", "record_id_r"]
    assert len(df) == 0


def test_sample_all_pairs_warns(table_factory):
    t = table_factory({"record_id": range(100_000)})
    with pytest.warns(UserWarning):
        sample_all_links(t, t)


def test_sample_all_pairs_different_tables(table_factory):
    # check that we can sample from two different tables
    # https://github.com/NickCrews/mismo/issues/35#issuecomment-2116178262
    t = table_factory({"record_id": range(100)})
    df = sample_all_links(t, t.view(), max_pairs=10).execute()
    assert df.columns.tolist() == ["record_id_l", "record_id_r"]
    assert len(df) == 10
